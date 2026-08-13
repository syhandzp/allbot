from __future__ import annotations

import datetime
import sys
from typing import Any, Awaitable, Callable, Dict, List, Optional

from allbot_direct import Context, run_direct


def builtin_points_auth(price_config: str = "auth_price_per_month") -> Dict[str, Any]:
    return {"type": "builtin_points", "price_config": price_config}


def builtin_payment_auth(price_config: str = "auth_price_per_month") -> Dict[str, Any]:
    return {"type": "builtin_payment", "price_config": price_config}


def create_account_ql_plugin(options: Dict[str, Any]) -> None:
    AccountQLPlugin(options).run()


class AccountQLPlugin:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.prefix = str(options.get("prefix") or "").strip()
        self.table_name = str(options.get("table_name") or options.get("tableName") or "").strip()
        ql = options.get("ql") or {}
        self.env_name = str(options.get("env_name") or options.get("envName") or ql.get("env_name") or ql.get("envName") or "").strip()
        self.account = options.get("account") or {}
        self.auth = options.get("auth") or {"provider": builtin_points_auth()}
        self.ql = ql
        self.schedules = options.get("schedules") or {}
        self.routes = options.get("routes") or {}
        if not self.prefix:
            raise RuntimeError("prefix 不能为空")
        if not self.table_name:
            raise RuntimeError("table_name 不能为空")
        if not self.env_name:
            raise RuntimeError("env_name 不能为空")

    def run(self) -> None:
        run_direct(self.handle)

    def store(self, ctx: Context) -> AccountStore:
        return AccountStore(ctx, table_name=self.table_name)

    def helpers(self, ctx: Context) -> AccountQLHelpers:
        return AccountQLHelpers(self, ctx)

    async def handle(self, ctx: Context) -> None:
        await self.ensure_schedules(ctx)
        content = str(ctx.content or "").strip()
        if not content.startswith(self.prefix):
            await ctx.reply(self.help_text())
            return
        suffix = content[len(self.prefix):].strip() or "帮助"
        helpers = self.helpers(ctx)
        if suffix in self.routes:
            await maybe_await(self.routes[suffix](ctx, helpers))
        elif suffix == "登录":
            await self.login(ctx)
        elif suffix in ("账号", "管理"):
            await self.list_accounts(ctx)
        elif suffix == "查询":
            await self.query_mine(ctx)
        elif suffix == "运行":
            await self.run_task(ctx, False)
        elif suffix in ("一键运行", "签到"):
            await self.run_task(ctx, True)
        elif suffix == "授权":
            await self.grant_auth(ctx)
        elif suffix == "删除":
            await self.delete_by_menu(ctx)
        elif suffix == "CK检测":
            await self.check_ck(ctx)
        elif suffix == "过期检测":
            await self.check_expirations(ctx)
        else:
            await ctx.reply(self.help_text())

    async def login(self, ctx: Context) -> None:
        custom_login = self.options.get("login")
        if callable(custom_login):
            await custom_login(ctx, self.helpers(ctx))
            return
        parser = self.account.get("parse_input") or self.account.get("parseInput")
        if not callable(parser):
            await ctx.reply("该插件没有配置登录处理")
            return
        await ctx.reply(self.options.get("login_prompt") or f"请发送{self.prefix}账号 CK，回复 q 退出：")
        raw = (await ctx.listen(120)).strip()
        if not raw or raw.lower() == "q":
            await ctx.reply("已取消登录")
            return
        data = await maybe_await(parser(raw, ctx))
        saved = await self.save_account(ctx, data)
        await ctx.reply(f"✅{'覆盖更新' if saved['existing'] else '添加'}成功：{saved['account'].get('account_name')}\n发送【{self.prefix}授权】授权后即可运行。")

    async def save_account(self, ctx: Context, data: Any, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        normalized = await self.normalize_input(data, ctx)
        store = self.store(ctx)
        existing = await self.find_existing(store, normalized)
        metadata = dict((existing or {}).get("metadata") or {})
        metadata.update(normalized.get("metadata") or {})
        metadata["account_key"] = normalized["unique_key"]
        metadata["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        metadata.update((extra or {}).get("metadata") or {})
        account = await store.save(
            id=(existing or {}).get("id") or 0,
            union_id=ctx.union_id,
            platform=ctx.platform,
            user_id=ctx.user_id,
            account_name=(existing or {}).get("account_name") or normalized["display_name"],
            env_name=self.env_name,
            env_value=normalized["env_value"],
            remark=normalized.get("remark") or (existing or {}).get("remark") or "",
            status=(existing or {}).get("status") or "active",
            metadata=metadata,
            expires_at=(existing or {}).get("expires_at") or "",
        )
        return {"account": account, "existing": bool(existing), "existing_account": existing, "existing_expires_at": (existing or {}).get("expires_at") or ""}

    async def normalize_input(self, data: Any, ctx: Context) -> Dict[str, Any]:
        value = {"env_value": data} if isinstance(data, str) else dict(data or {})
        env_value = str(value.get("env_value") or value.get("envValue") or value.get("value") or "").strip()
        unique_key = str(value.get("unique_key") or value.get("uniqueKey") or "").strip()
        if not unique_key:
            generator = self.account.get("unique_key") or self.account.get("uniqueKey")
            if callable(generator):
                unique_key = str(await maybe_await(generator(value, ctx))).strip()
        display_name = str(value.get("display_name") or value.get("displayName") or value.get("account_name") or value.get("accountName") or "").strip()
        if not display_name:
            generator = self.account.get("display_name") or self.account.get("displayName")
            if callable(generator):
                display_name = str(await maybe_await(generator(value, ctx))).strip()
        if not display_name:
            display_name = unique_key or env_value[:8]
        if not env_value:
            raise RuntimeError("账号 CK 不能为空")
        if not unique_key:
            raise RuntimeError("账号唯一键不能为空")
        allow_multiline = bool(self.account.get("allow_multiline") or self.account.get("allowMultiline"))
        if "\x00" in env_value or (not allow_multiline and ("\n" in env_value or "\r" in env_value)):
            raise RuntimeError("单个账号 CK 不能包含空字符或换行")
        return {**value, "env_value": env_value, "unique_key": unique_key, "display_name": display_name, "remark": str(value.get("remark") or "").strip(), "metadata": value.get("metadata") or {}}

    async def find_existing(self, store: AccountStore, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        accounts = await store.list_mine(env_name=self.env_name)
        for account in accounts:
            if (account.get("metadata") or {}).get("account_key") == data["unique_key"] or account.get("env_value") == data["env_value"]:
                return account
        return None

    async def list_accounts(self, ctx: Context) -> None:
        accounts = await self.store(ctx).list_mine(env_name=self.env_name)
        if not accounts:
            await ctx.reply(f"暂无账号，请发送【{self.prefix}登录】添加。")
            return
        lines = [f"{i + 1}. {self.account_name(item)}｜{format_auth_status(account_expires_at(item))}" for i, item in enumerate(accounts)]
        await self.send_buttons(ctx, f"====={self.prefix}账号管理=====\n" + "\n".join(lines) + "\n------------------\n回复序号可操作账号，回复 q 退出：", self.account_choice_buttons(accounts))
        choice = (await ctx.listen(60)).strip().lower()
        if choice == "q":
            await ctx.reply("已退出账号管理")
            return
        index = parse_int(choice, 0) - 1
        if index < 0 or index >= len(accounts):
            await ctx.reply("❌账号序号错误")
            return
        account = accounts[index]
        await self.send_buttons(ctx, f"当前账号：{self.account_name(account)}\n[1] 授权账号\n[2] 删除账号\n[3] 运行当前账号\n回复 q 退出：", [
            [self.user_button(ctx, "授权账号", "1"), self.user_button(ctx, "运行当前账号", "3")],
            [self.user_button(ctx, "删除账号", "2")],
            [self.user_button(ctx, "退出", "q")],
        ])
        action = (await ctx.listen(60)).strip().lower()
        if action == "1":
            await self.authorize_account(ctx, account)
        elif action == "2":
            await self.delete_account(ctx, account)
        elif action == "3":
            if not is_authorized(account_expires_at(account)):
                await ctx.reply(f"❌{self.account_name(account)}未授权或已过期，请先授权。")
                return
            await self.run_accounts(ctx, [account], "single_account", f"{self.prefix}账号运行：{self.account_name(account)}")
        else:
            await ctx.reply("已退出账号管理")

    async def delete_by_menu(self, ctx: Context) -> None:
        accounts = await self.store(ctx).list_mine(env_name=self.env_name)
        if not accounts:
            await ctx.reply("暂无账号可删除。")
            return
        await self.send_buttons(ctx, "请选择要删除的账号：\n" + "\n".join([f"{i + 1}. {self.account_name(item)}" for i, item in enumerate(accounts)]) + "\n回复 q 退出：", self.account_choice_buttons(accounts))
        choice = (await ctx.listen(60)).strip().lower()
        if choice == "q":
            await ctx.reply("已取消删除")
            return
        index = parse_int(choice, 0) - 1
        if index < 0 or index >= len(accounts):
            await ctx.reply("❌账号序号错误")
            return
        await self.delete_account(ctx, accounts[index])

    async def delete_account(self, ctx: Context, account: Dict[str, Any]) -> None:
        await self.send_buttons(ctx, f"确认删除账号【{self.account_name(account)}】吗？回复 y 确认：", [[self.user_button(ctx, "确认删除", "y")], [self.user_button(ctx, "取消", "q")]])
        if (await ctx.listen(30)).strip().lower() != "y":
            await ctx.reply("已取消删除")
            return
        await self.store(ctx).delete(account.get("id"))
        await ctx.reply(f"✅已删除账号：{self.account_name(account)}")

    async def grant_auth(self, ctx: Context) -> None:
        accounts = await self.store(ctx).list_mine(env_name=self.env_name)
        if not accounts:
            await ctx.reply(f"暂无账号，请先发送【{self.prefix}登录】添加。")
            return
        if len(accounts) == 1:
            await self.authorize_account(ctx, accounts[0])
            return
        await self.send_buttons(ctx, "请选择要授权的账号：\n" + "\n".join([f"{i + 1}. {self.account_name(item)}｜{format_auth_status(account_expires_at(item))}" for i, item in enumerate(accounts)]), self.account_choice_buttons(accounts))
        index = parse_int((await ctx.listen(60)).strip(), 0) - 1
        if index < 0 or index >= len(accounts):
            await ctx.reply("❌账号序号错误")
            return
        await self.authorize_account(ctx, accounts[index])

    async def authorize_account(self, ctx: Context, account: Dict[str, Any]) -> None:
        provider = self.auth.get("provider") or builtin_points_auth()
        price = max(0, parse_int(ctx.config(provider.get("price_config", "auth_price_per_month"), 0), 0))
        unit = ctx.points_unit or "积分"
        await self.send_buttons(ctx, f"请输入授权月数（必须大于 0）{'（' + str(price) + unit + '/月）' if price > 0 else '（免费）'}：", [[self.user_button(ctx, "1个月", "1"), self.user_button(ctx, "3个月", "3"), self.user_button(ctx, "12个月", "12")], [self.user_button(ctx, "取消", "q")]])
        raw = (await ctx.listen(60)).strip().lower()
        if raw == "q":
            await ctx.reply("已取消授权")
            return
        months = parse_int(raw, 0)
        if months <= 0:
            await ctx.reply("❌授权月数必须大于 0")
            return
        cost = price * months
        if cost > 0:
            provider_type = str(provider.get("type") or "builtin_points")
            if provider_type == "builtin_payment":
                points_per_rmb = max(1, parse_int(ctx.config("auth_payment_points_per_rmb", 100), 100))
                amount_rmb = cost / points_per_rmb
                timeout = max(1, parse_int(ctx.config("auth_payment_timeout", 300), 300))
                methods = [item.strip() for item in str(ctx.config("auth_payment_methods", "")).split(",") if item.strip()]
                await self.send_buttons(ctx, f"本次授权需要支付 {amount_rmb:.2f} 元（{cost}{unit}），请输入 y 发起支付：", [[self.user_button(ctx, "发起支付", "y")], [self.user_button(ctx, "取消", "q")]])
                if (await ctx.listen(30)).strip().lower() != "y":
                    await ctx.reply("已取消授权")
                    return
                order = await ctx.pay.wait_pay(f"{self.prefix}账号授权", f"{amount_rmb:.2f}", timeout=timeout, methods=methods, metadata={"plugin": self.prefix, "account_id": account.get("id"), "months": months})
                if str(order.get("status") or "") != "paid":
                    await ctx.reply("❌支付未完成，授权已取消。")
                    return
            else:
                await self.send_buttons(ctx, f"本次授权需要扣除 {cost}{unit}，当前 {ctx.points}{unit}，回复 y 确认：", [[self.user_button(ctx, "确认授权", "y")], [self.user_button(ctx, "取消", "q")]])
                if (await ctx.listen(30)).strip().lower() != "y":
                    await ctx.reply("已取消授权")
                    return
                await ctx.consume_points(cost)
        base = max(datetime.datetime.now(datetime.timezone.utc).timestamp(), safe_timestamp(parse_time(account_expires_at(account))))
        expires_at = datetime.datetime.fromtimestamp(base + months * 30 * 86400, tz=datetime.timezone.utc)
        expires_text = expires_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        await self.store(ctx).save(**{**account, "id": account.get("id"), "expires_at": expires_text, "env_name": account.get("env_name"), "env_value": account.get("env_value"), "account_name": account.get("account_name")})
        await ctx.reply(f"✅授权成功：{self.account_name(account)}，到期时间 {format_time(expires_text)}")

    async def query_mine(self, ctx: Context) -> None:
        accounts = await self.store(ctx).list_mine(env_name=self.env_name, status="active")
        if not accounts:
            await ctx.reply(f"暂无账号，请发送【{self.prefix}登录】添加。")
            return
        if len(accounts) == 1:
            await self.reply_queries(ctx, accounts, separate=False)
            return
        selected = await self.select_accounts_for_query(ctx, accounts)
        if not selected:
            await ctx.reply("已取消查询")
            return
        await self.reply_queries(ctx, selected["accounts"], separate=selected["all"])

    def supports_buttons(self, ctx: Context) -> bool:
        sender = getattr(ctx, "sendButtons", None) or getattr(ctx, "send_buttons", None)
        return bool(getattr(ctx, "platform", "") in ["telegram", "qq_office"] and callable(sender))

    def build_query_selection_buttons(self, page_accounts: List[Dict[str, Any]], start: int, total_pages: int, page: int, user_id: str = "") -> List[List[Dict[str, str]]]:
        restrict_user_id = str(user_id or "").strip()

        def button(text: str, value: str) -> Dict[str, str]:
            item = {"text": text, "value": value}
            if restrict_user_id:
                item["user_id"] = restrict_user_id
            return item

        rows: List[List[Dict[str, str]]] = [[button("[0] 全部查询", "0")]]
        for index in range(0, len(page_accounts), 4):
            row: List[Dict[str, str]] = []
            for offset, account in enumerate(page_accounts[index:index + 4]):
                number = start + index + offset + 1
                row.append(button(f"[{number}] {self.account_name(account)}", str(number)))
            if row:
                rows.append(row)
        if total_pages > 1:
            rows.append([
                button("上一页", "p"),
                button(f"第 {page + 1}/{total_pages} 页", "page"),
                button("下一页", "n"),
            ])
            rows.append([button("退出", "q")])
        else:
            rows.append([button("退出", "q")])
        return rows

    async def send_query_selection_prompt(self, ctx: Context, text: str, buttons: List[List[Dict[str, str]]]) -> None:
        if self.supports_buttons(ctx):
            sender = getattr(ctx, "sendButtons", None) or getattr(ctx, "send_buttons", None)
            await sender(text, buttons)
            return
        await ctx.reply(text)

    async def select_accounts_for_query(self, ctx: Context, accounts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        page_size = 20
        total_pages = (len(accounts) + page_size - 1) // page_size
        page = 0
        while True:
            start = page * page_size
            page_accounts = accounts[start:start + page_size]
            lines = ["请输入要查询的账号：", "[0] 全部查询", "--------------------"]
            lines.extend([f"[{start + i + 1}] {self.account_name(item)}" for i, item in enumerate(page_accounts)])
            if total_pages > 1:
                lines.extend(["--------------------", f"第 {page + 1}/{total_pages} 页，回复 n 下一页，p 上一页，q 退出"])
            await self.send_query_selection_prompt(ctx, "\n".join(lines), self.build_query_selection_buttons(page_accounts, start, total_pages, page, getattr(ctx, "userId", "") or getattr(ctx, "user_id", "")))
            choice = (await ctx.listen(60)).strip().lower()
            if not choice or choice == "q":
                return None
            if choice == "0":
                return {"all": True, "accounts": accounts}
            if choice == "page":
                continue
            if total_pages > 1 and choice == "n":
                page = (page + 1) % total_pages
                continue
            if total_pages > 1 and choice == "p":
                page = (page - 1 + total_pages) % total_pages
                continue
            index = parse_int(choice, 0) - 1
            if index < 0 or index >= len(accounts):
                await ctx.reply("❌账号序号错误")
                return None
            return {"all": False, "accounts": [accounts[index]]}

    async def send_buttons(self, ctx: Context, text: str, buttons: List[List[Dict[str, str]]]) -> None:
        sender = getattr(ctx, "send_buttons", None) or getattr(ctx, "sendButtons", None)
        if callable(sender):
            await sender(text, buttons)
            return
        await ctx.reply(text)

    def user_button(self, ctx: Context, text: str, value: str) -> Dict[str, str]:
        button = {"text": text, "value": value}
        user_id = str(getattr(ctx, "user_id", "") or getattr(ctx, "userId", "") or "").strip()
        if user_id:
            button["user_id"] = user_id
        return button

    def account_choice_buttons(self, accounts: List[Dict[str, Any]]) -> List[List[Dict[str, str]]]:
        rows: List[List[Dict[str, str]]] = []
        for start in range(0, len(accounts), 4):
            row: List[Dict[str, str]] = []
            for offset, account in enumerate(accounts[start:start + 4]):
                number = start + offset + 1
                row.append({"text": f"{number}. {short_text(self.account_name(account), 12)}", "value": str(number)})
            rows.append(row)
        rows.append([{"text": "退出", "value": "q"}])
        return rows

    async def reply_queries(self, ctx: Context, accounts: List[Dict[str, Any]], separate: bool) -> None:
        query = self.account.get("query")
        if not callable(query):
            await ctx.reply("该插件没有配置查询功能")
            return
        if separate:
            for index, account in enumerate(accounts):
                await ctx.reply(await self.query_text(ctx, account, index))
        else:
            await ctx.reply("\n".join([await self.query_text(ctx, item, i) for i, item in enumerate(accounts)]))

    async def query_text(self, ctx: Context, account: Dict[str, Any], index: int) -> str:
        try:
            result = await maybe_await(self.account["query"](account, ctx, index))
            if isinstance(result, str):
                return result
            parts = "｜".join([f"{k}：{v}" for k, v in (result or {}).items()])
            return f"{self.account_name(account)}｜{parts}｜到期：{format_auth_status(account_expires_at(account))}"
        except Exception as error:
            return f"{self.account_name(account)}｜查询失败：{error}｜到期：{format_auth_status(account_expires_at(account))}"

    async def run_task(self, ctx: Context, all_users: bool) -> None:
        if all_users and not ctx.is_admin() and ctx.meta("fake") != "true":
            await ctx.reply(f"❌{self.prefix}一键运行仅平台管理员或定时任务可用。")
            return
        store = self.store(ctx)
        accounts = await (store.list_all if all_users else store.list_mine)(env_name=self.env_name, status="active")
        runnable = [item for item in accounts if is_authorized(account_expires_at(item))]
        if not runnable:
            await ctx.reply(f"暂无已授权账号，请先发送【{self.prefix}授权】选择账号授权。")
            return
        await self.run_accounts(ctx, runnable, "all_authorized" if all_users else "current_user", f"{self.prefix}一键运行" if all_users else f"{self.prefix}运行")

    async def run_accounts(self, ctx: Context, accounts: List[Dict[str, Any]], run_mode: str, title: str) -> None:
        is_scheduled = ctx.meta("fake") == "true"
        if "wait_scheduled" in self.ql:
            wait_scheduled = self.ql.get("wait_scheduled")
        else:
            wait_scheduled = self.ql.get("waitScheduled")
        wait = wait_scheduled is True if is_scheduled else True
        if wait and not is_scheduled:
            await ctx.reply(f"🚀开始执行{title}，共 {len(accounts)} 个账号。")
        started_at = datetime.datetime.now(datetime.timezone.utc)
        timeout_config = self.ql.get("timeout_config") or self.ql.get("timeoutConfig") or "run_wait_timeout"
        timeout = max(1, parse_int(ctx.config(timeout_config, self.ql.get("timeout", 7200)), parse_int(self.ql.get("timeout"), 7200)))
        env_option = self.ql.get("env", {})
        env = await maybe_await(env_option(ctx, accounts)) if callable(env_option) else env_option
        runtime_config = self.ql.get("runtime_config") or self.ql.get("runtimeConfig") or "script_runtime"
        script_runtime = ctx.config(runtime_config, self.ql.get("runtime", "python")) or self.ql.get("runtime", "python")
        result = await ctx.run_ql_script(runtime=script_runtime, script=ctx.config(self.ql.get("script_config") or self.ql.get("scriptConfig") or "task_script", self.ql.get("script", "")), env_name=self.env_name, accounts=accounts, run_mode=run_mode, timeout=timeout, wait=wait, env=env or {})
        if not wait:
            await ctx.reply(script_task_message(result, f"{title}任务已提交"))
            return
        if result.get("already_running") and result.get("status") == "running":
            if run_mode != "all_authorized" or ctx.config("notify_scheduled") == "true":
                await ctx.reply(script_task_message(result, f"{title}任务正在运行"))
            return
        if result.get("timeout"):
            if run_mode != "all_authorized" or ctx.config("notify_scheduled") == "true":
                await ctx.reply(script_task_message(result, f"{title}仍在运行"))
            return
        if result.get("status") != "success":
            elapsed_ms = (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds() * 1000
            if run_mode != "all_authorized" or ctx.config("notify_scheduled") == "true":
                await ctx.reply(self.run_summary_message(result, len(accounts), elapsed_ms))
            return
        if run_mode == "all_authorized":
            await self.call_after_run(ctx, accounts, result, run_mode, title, is_scheduled)
        await self.check_ck_after_run(ctx, accounts)
        if run_mode != "all_authorized" and self.account.get("query"):
            await self.reply_queries(ctx, accounts, separate=True)
        elif run_mode == "all_authorized":
            elapsed_ms = (datetime.datetime.now(datetime.timezone.utc) - started_at).total_seconds() * 1000
            if ctx.config("notify_scheduled") == "true":
                await ctx.reply(self.run_summary_message(result, len(accounts), elapsed_ms))
        else:
            await ctx.reply(f"✅执行完成：{title}")

    def run_summary_message(self, result: Dict[str, Any], account_count: int, elapsed_ms: float) -> str:
        task_id = result.get("task_id") or result.get("log_id") or result.get("id") or ""
        if result.get("status") != "success":
            error = result.get("error") or ""
            lines = [f"❌{self.prefix}生活运行失败！共运行{account_count}个账号，耗时{format_duration_seconds(elapsed_ms)}"]
            if task_id:
                lines.append(f"任务ID：{task_id}")
            if error:
                lines.append(f"错误：{error}")
            output = script_output_tail(result.get("output"))
            if output:
                lines.append(f"最后输出：\n{output}")
            lines.append("请到后台【脚本任务】查看完整日志。")
            return "\n".join(lines)
        text = f"✅{self.prefix}生活运行完成！共运行{account_count}个账号，耗时{format_duration_seconds(elapsed_ms)}"
        if task_id:
            text += f"\n任务ID：{task_id}"
        return text

    async def call_after_run(self, ctx: Context, accounts: List[Dict[str, Any]], result: Dict[str, Any], run_mode: str, title: str, is_scheduled: bool) -> None:
        after_run = self.ql.get("after_run") or self.ql.get("afterRun")
        if not callable(after_run):
            return
        helpers = self.helpers(ctx)
        helpers.run_mode = run_mode
        helpers.runMode = run_mode
        helpers.title = title
        helpers.is_scheduled = is_scheduled
        helpers.isScheduled = is_scheduled
        try:
            await maybe_await(after_run(ctx, accounts, result, helpers))
        except Exception as error:
            print(f"{self.prefix} after_run 执行失败：{error}", file=sys.stderr)

    async def check_ck_after_run(self, ctx: Context, accounts: List[Dict[str, Any]]) -> None:
        checker = self.account.get("check_ck") or self.account.get("checkCk")
        if not callable(checker):
            return
        try:
            result = await self.store(ctx).scan_ck_status(accounts=accounts, checker=checker, title=f"{self.prefix} CK", message=lambda account, state: f"【{self.prefix}CK提醒】{self.account_name(account)} {state.get('reason') or 'CK 已失效'}，请发送【{self.prefix}登录】重新登录或更新 CK。")
            if result.get("invalid", 0) > 0:
                print(f"{self.prefix}CK检测：发现 {result.get('invalid')} 个失效账号，已通知 {result.get('notified')} 个。", file=sys.stderr)
        except Exception as error:
            print(f"{self.prefix}运行后CK检测失败：{error}", file=sys.stderr)

    async def check_ck(self, ctx: Context) -> None:
        if not ctx.is_admin() and ctx.meta("fake") != "true":
            await ctx.reply(f"❌{self.prefix}CK检测仅平台管理员或定时任务可用。")
            return
        checker = self.account.get("check_ck") or self.account.get("checkCk")
        result = await self.store(ctx).scan_ck_status(env_name=self.env_name, checker=checker, title=f"{self.prefix} CK", message=lambda account, state: f"【{self.prefix}CK提醒】{self.account_name(account)} {state.get('reason') or 'CK 已失效'}，请发送【{self.prefix}登录】重新登录或更新 CK。")
        await ctx.reply(f"✅{self.prefix}CK检测完成：账号 {result.get('accounts')} 个，正常 {result.get('valid')} 个，失效 {result.get('invalid')} 个，通知 {result.get('notified')} 个，异常 {result.get('errors')} 个。")

    async def check_expirations(self, ctx: Context) -> None:
        if not ctx.is_admin() and ctx.meta("fake") != "true":
            await ctx.reply(f"❌{self.prefix}过期检测仅平台管理员或定时任务可用。")
            return
        notify_days = parse_days(ctx.config("expire_notify_days", "7,3,1,0"))
        delete_after_days = parse_int(ctx.config("expire_delete_after_days", -1), -1)
        result = await self.store(ctx).scan_expirations(
            env_name=self.env_name,
            notify_days=notify_days,
            delete_after_days=delete_after_days,
            title=f"{self.prefix}账号授权",
            unauthorized_message=lambda account, state: f"【{self.prefix}账号授权提醒】{self.account_name(account)} 尚未授权" + (f"（已添加 {state.get('days_since_created')} 天）" if state.get("days_since_created") is not None else "") + f"，请发送【{self.prefix}账号】完成授权。",
            message=lambda account, state: expiration_message(self.prefix, self.account_name(account), state),
        )
        await ctx.reply(f"✅{self.prefix}过期检测完成：账号 {result.get('accounts')} 个，通知 {result.get('notified')} 个，删除 {result.get('deleted')} 个，跳过 {result.get('skipped')} 个。")

    async def ensure_schedules(self, ctx: Context) -> None:
        try:
            admins = await ctx.list_platform_admins()
        except Exception as error:
            print(f"声明{self.prefix}定时任务失败：获取管理员身份失败：{error}", file=sys.stderr)
            return
        admin = admins[0] if admins else None
        if not admin:
            print(f"声明{self.prefix}定时任务失败：没有已启动平台的管理员身份", file=sys.stderr)
            return
        normalized_schedules = normalize_schedules(self.prefix, self.schedules)
        default_max_count = max(3, len(normalized_schedules))
        for item in normalized_schedules:
            try:
                await ctx.set_scheduled_task(task_key=item["task_key"], name=item["name"], description=item["description"], cron=str(ctx.config(item["cron_config"], item["cron"])), platform=str(admin.get("platform") or ""), adapter_id=str(admin.get("adapter_id") or ""), user_id=str(admin.get("user_id") or ""), group_id="", content=item["content"], max_count=max(item.get("max_count") or 0, default_max_count))
            except Exception as error:
                print(f"声明定时任务失败：{item.get('task_key')} {error}", file=sys.stderr)

    def account_name(self, account: Dict[str, Any]) -> str:
        return str(account.get("account_name") or account.get("remark") or account.get("env_value", "")[:8] or "未知账号")

    def help_text(self) -> str:
        commands = [f"{self.prefix}登录", f"{self.prefix}账号", f"{self.prefix}查询", f"{self.prefix}运行", f"{self.prefix}一键运行", f"{self.prefix}授权", f"{self.prefix}删除"]
        if callable(self.account.get("check_ck") or self.account.get("checkCk")):
            commands.append(f"{self.prefix}CK检测")
        if self.schedules.get("expire_check") or self.schedules.get("expireCheck"):
            commands.append(f"{self.prefix}过期检测")
        for command in self.routes.keys():
            commands.append(f"{self.prefix}{command}")
        return "支持指令：" + " / ".join(commands)


class AccountQLHelpers:
    def __init__(self, plugin: AccountQLPlugin, ctx: Context):
        self.plugin = plugin
        self.ctx = ctx
        self.prefix = plugin.prefix
        self.env_name = plugin.env_name
        self.envName = plugin.env_name
        self.table_name = plugin.table_name
        self.tableName = plugin.table_name
        self.store = plugin.store(ctx)

    async def save_account(self, data: Any, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.plugin.save_account(self.ctx, data, extra)

    saveAccount = save_account

    async def list_mine(self, **options: Any) -> List[Dict[str, Any]]:
        return await self.store.list_mine(**{**options, "env_name": options.get("env_name") or options.get("envName") or self.plugin.env_name})

    listMine = list_mine

    async def list_all(self, **options: Any) -> List[Dict[str, Any]]:
        return await self.store.list_all(**{**options, "env_name": options.get("env_name") or options.get("envName") or self.plugin.env_name})

    listAll = list_all

    def account_name(self, account: Dict[str, Any]) -> str:
        return self.plugin.account_name(account)

    accountName = account_name

    def format_auth_status(self, value: str) -> str:
        return format_auth_status(value)

    formatAuthStatus = format_auth_status

    def is_authorized(self, account: Dict[str, Any]) -> bool:
        return is_authorized(account_expires_at(account))

    isAuthorized = is_authorized

    async def run_account(self, account: Dict[str, Any]) -> None:
        if not self.is_authorized(account):
            await self.ctx.reply(f"❌{self.account_name(account)}未授权或已过期，请先授权。")
            return
        await self.plugin.run_accounts(self.ctx, [account], "single_account", f"{self.plugin.prefix}账号运行：{self.account_name(account)}")

    runAccount = run_account

    async def query_accounts(self, accounts: List[Dict[str, Any]], title: str = "账号查询结果", **options: Any) -> None:
        await self.plugin.reply_queries(self.ctx, accounts, separate=options.get("separate", True))

    queryAccounts = query_accounts

    def __getattr__(self, name: str) -> Any:
        return getattr(self.plugin, name)


class AccountStore:
    def __init__(self, ctx: Context, **options: Any):
        self.ctx = ctx
        self.table_name = str(options.get("table_name") or options.get("tableName") or "")

    async def save(self, **account: Any) -> Dict[str, Any]:
        return self.ctx._request({
            "action": "account_save",
            "table_name": str(account.get("table_name") or account.get("tableName") or self.table_name),
            "id": int(account.get("id") or 0),
            "union_id": str(account.get("union_id") or account.get("unionId") or self.ctx.union_id),
            "platform": str(account.get("platform") or self.ctx.platform),
            "user_id": str(account.get("user_id") or account.get("userId") or self.ctx.user_id),
            "account_name": str(account.get("account_name") or account.get("accountName") or account.get("name") or ""),
            "env_name": str(account.get("env_name") or account.get("envName") or ""),
            "env_value": str(account.get("env_value") or account.get("envValue") or account.get("value") or ""),
            "remark": str(account.get("remark") or ""),
            "status": str(account.get("status") or "active"),
            "metadata": account.get("metadata") or {},
            "expires_at": str(account.get("expires_at") or account.get("expiresAt") or ""),
        }, "account_response")

    async def list(self, **options: Any) -> List[Dict[str, Any]]:
        return self.ctx._request({
            "action": "account_list",
            "table_name": str(options.get("table_name") or options.get("tableName") or self.table_name),
            "scope": str(options.get("scope") or "mine"),
            "union_id": str(options.get("union_id") or options.get("unionId") or self.ctx.union_id),
            "env_name": str(options.get("env_name") or options.get("envName") or ""),
            "status": str(options.get("status") or ""),
        }, "account_response")

    async def list_mine(self, **options: Any) -> List[Dict[str, Any]]:
        return await self.list(**{**options, "scope": "mine"})

    async def list_all(self, **options: Any) -> List[Dict[str, Any]]:
        return await self.list(**{**options, "scope": "all"})

    async def delete(self, account_id: Any, **options: Any) -> bool:
        self.ctx._request({
            "action": "account_delete",
            "table_name": str(options.get("table_name") or options.get("tableName") or self.table_name),
            "scope": str(options.get("scope") or "mine"),
            "id": int(account_id or 0),
            "union_id": str(options.get("union_id") or options.get("unionId") or self.ctx.union_id),
        }, "account_response")
        return True

    async def scan_expirations(self, **options: Any) -> Dict[str, int]:
        now = datetime.datetime.now(datetime.timezone.utc)
        notify_days = parse_days(options.get("notify_days") if "notify_days" in options else options.get("notifyDays", [7, 3, 1, 0]))
        delete_after_days = parse_int(options.get("delete_after_days") if "delete_after_days" in options else options.get("deleteAfterDays", -1), -1)
        accounts = await self.list_all(env_name=options.get("env_name") or options.get("envName") or "", status=options.get("status") or "active")
        result = {"notified": 0, "deleted": 0, "skipped": 0, "accounts": len(accounts)}
        for account in accounts:
            expires_at = account_expires_at(account)
            if not expires_at:
                created_at = parse_time(str(account.get("created_at") or account.get("createdAt") or ""))
                days_since_created = None if is_min_time(created_at) else max(0, int((now - created_at).total_seconds() // 86400))
                unauthorized_expired = delete_after_days >= 0 and days_since_created is not None and days_since_created >= delete_after_days
                if options.get("notify", True) is not False:
                    message = options.get("unauthorized_message") or options.get("unauthorizedMessage")
                    text = await maybe_await(message(account, {"days_since_created": days_since_created}) if callable(message) else f"【{options.get('title') or '账号授权'}提醒】{self.account_name(account)} 尚未授权。")
                    if await self.notify_account(account, text):
                        result["notified"] += 1
                elif not unauthorized_expired:
                    result["skipped"] += 1
                if unauthorized_expired and options.get("delete_expired", options.get("deleteExpired", True)) is not False:
                    await self.delete(account.get("id"), scope="all", union_id=account.get("union_id") or account.get("unionId"))
                    result["deleted"] += 1
                continue
            expires_time = parse_time(expires_at)
            if is_min_time(expires_time):
                result["skipped"] += 1
                continue
            days_left = int(((expires_time - now).total_seconds() + 86399) // 86400)
            notify_matched = days_left in notify_days or (days_left < 0 and 0 in notify_days)
            should_delete = delete_after_days >= 0 and (now - expires_time).total_seconds() >= delete_after_days * 86400
            if (notify_matched or should_delete) and options.get("notify", True) is not False:
                message = options.get("message")
                text = await maybe_await(message(account, {"days_left": days_left, "expires_at": expires_at}) if callable(message) else f"{options.get('title') or '账号授权'} {'将在 ' + str(days_left) + ' 天后过期' if days_left >= 0 else '已过期'}")
                if await self.notify_account(account, text):
                    result["notified"] += 1
            if should_delete and options.get("delete_expired", options.get("deleteExpired", True)) is not False:
                await self.delete(account.get("id"), scope="all", union_id=account.get("union_id") or account.get("unionId"))
                result["deleted"] += 1
        return result

    def account_name(self, account: Dict[str, Any]) -> str:
        return str(account.get("account_name") or account.get("accountName") or account.get("remark") or account.get("env_value", "")[:8] or "未知账号")

    async def notify_account(self, account: Dict[str, Any], text: str) -> bool:
        try:
            await self.ctx.send_message(union_id=account.get("union_id") or account.get("unionId") or "", platform=account.get("platform") or self.ctx.platform, user_id=account.get("user_id") or account.get("userId") or "", text=text)
            return True
        except Exception:
            return False

    async def scan_ck_status(self, **options: Any) -> Dict[str, int]:
        checker = options.get("checker") or options.get("check")
        if not callable(checker):
            raise RuntimeError("scan_ck_status 需要传入 checker(account) 函数")
        accounts = options.get("accounts")
        if not isinstance(accounts, list):
            accounts = await self.list_all(env_name=options.get("env_name") or options.get("envName") or "", status=options.get("status") or "active")
        result = {"accounts": len(accounts), "checked": 0, "valid": 0, "invalid": 0, "notified": 0, "skipped": 0, "errors": 0}
        for account in accounts:
            try:
                state = await maybe_await(checker(account, self.ctx))
                result["checked"] += 1
                valid = bool(state if isinstance(state, bool) else (state or {}).get("valid"))
                if valid:
                    result["valid"] += 1
                else:
                    result["invalid"] += 1
                    if options.get("notify", True) is not False:
                        message = options.get("message")
                        text = await maybe_await(message(account, state or {}) if callable(message) else f"【{options.get('title') or '账号 CK'}提醒】{self.account_name(account)} CK 已失效。")
                        if await self.notify_account(account, text):
                            result["notified"] += 1
            except Exception:
                result["errors"] += 1
                result["skipped"] += 1
        return result


async def maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def script_task_message(result: Dict[str, Any], fallback: str) -> str:
    task_id = result.get("task_id") or result.get("log_id") or result.get("id") or ""
    status = "任务已在运行" if result.get("already_running") else "任务已创建"
    text = f"✅{fallback or status}"
    if task_id:
        text += f"\n任务ID：{task_id}"
    return text + "\n请到后台【脚本任务】查看运行状态和日志。"


def script_output_tail(output: Any, max_lines: int = 8, max_chars: int = 800) -> str:
    text = str(output or "").strip()
    if not text:
        return ""
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    text = "\n".join(lines).strip()
    if len(text) > max_chars:
        text = "..." + text[-max_chars:]
    return text


def normalize_schedules(prefix: str, schedules: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    run_items = []
    if schedules.get("run"):
        run_items.append(schedules["run"])
    extra_runs = schedules.get("runs") or schedules.get("run_list") or schedules.get("runList") or []
    if isinstance(extra_runs, list):
        run_items.extend(extra_runs)
    for item in run_items:
        result.append({"task_key": item.get("task_key") or item.get("taskKey") or f"{prefix}-default-run", "name": item.get("name", f"{prefix}自动运行"), "description": item.get("description", "默认脚本运行任务"), "cron_config": item.get("cron_config") or item.get("cronConfig") or "cron", "cron": item.get("cron", "0 8 * * *"), "content": item.get("content", f"{prefix}一键运行"), "max_count": item.get("max_count") or item.get("maxCount") or 3})
    expire_item = schedules.get("expire_check") or schedules.get("expireCheck")
    if expire_item:
        result.append({"task_key": expire_item.get("task_key") or expire_item.get("taskKey") or f"{prefix}-expiration-check", "name": expire_item.get("name", f"{prefix}过期检测"), "description": expire_item.get("description", "检测账号授权到期并提醒续费"), "cron_config": expire_item.get("cron_config") or expire_item.get("cronConfig") or "expire_check_cron", "cron": expire_item.get("cron", "15 9 * * *"), "content": expire_item.get("content", f"{prefix}过期检测"), "max_count": expire_item.get("max_count") or expire_item.get("maxCount") or 3})
    ck_item = schedules.get("ck_check") or schedules.get("ckCheck")
    if ck_item:
        result.append({"task_key": ck_item.get("task_key") or ck_item.get("taskKey") or f"{prefix}-ck-check", "name": ck_item.get("name", f"{prefix} CK 检测"), "description": ck_item.get("description", "检测账号 CK 是否失效"), "cron_config": ck_item.get("cron_config") or ck_item.get("cronConfig") or "ck_check_cron", "cron": ck_item.get("cron", "25 9 * * *"), "content": ck_item.get("content", f"{prefix}CK检测"), "max_count": ck_item.get("max_count") or ck_item.get("maxCount") or 3})
    return result


def short_text(value: Any, max_length: int) -> str:
    text = str(value or "")
    return text[:max_length] + "…" if len(text) > max_length else text


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_days(value: Any) -> List[int]:
    items = value if isinstance(value, list) else str(value or "").split(",")
    result = []
    for item in items:
        try:
            result.append(int(str(item).strip()))
        except (TypeError, ValueError):
            continue
    return result


def expiration_message(prefix: str, name: str, state: Dict[str, Any]) -> str:
    days_left = int(state.get("days_left") if state.get("days_left") is not None else state.get("daysLeft") or 0)
    if days_left > 0:
        return f"【{prefix}账号授权提醒】{name} 将在 {days_left} 天后过期，请发送【{prefix}账号】续费。"
    if days_left == 0:
        return f"【{prefix}账号授权提醒】{name} 今天到期，请发送【{prefix}账号】续费。"
    return f"【{prefix}账号授权提醒】{name} 已过期，请发送【{prefix}账号】续费后继续使用。"


def account_expires_at(account: Dict[str, Any]) -> str:
    return str(account.get("expires_at") or account.get("expiresAt") or "")


def is_min_time(value: datetime.datetime) -> bool:
    return value.astimezone(datetime.timezone.utc) == datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


def parse_time(value: str) -> datetime.datetime:
    if not value or str(value).startswith("0001-01-01"):
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    try:
        parsed = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc)
    except Exception:
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


def is_authorized(value: str) -> bool:
    return parse_time(value) > datetime.datetime.now(datetime.timezone.utc)


def format_time(value: str) -> str:
    date = parse_time(value)
    return "无" if is_min_time(date) else date.strftime("%Y-%m-%d")


def format_duration_seconds(ms: float) -> str:
    seconds = max(0.0, float(ms or 0)) / 1000
    return f"{seconds:.3f}秒"


def safe_timestamp(value: datetime.datetime) -> float:
    if is_min_time(value):
        return 0.0
    try:
        return value.timestamp()
    except OSError:
        return 0.0


def format_auth_status(value: str) -> str:
    if not value:
        return "未授权"
    return f"{format_time(value)}" if is_authorized(value) else f"已过期"


__all__ = ["create_account_ql_plugin", "builtin_points_auth", "builtin_payment_auth", "AccountQLPlugin", "AccountStore"]
