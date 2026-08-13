const { runDirect, PAY } = require('./allbot_direct');

function createAccountQLPlugin(options = {}) {
  const plugin = new AccountQLPlugin(options);
  return plugin.run();
}

function builtinPointsAuth(options = {}) {
  return {
    type: 'builtin_points',
    priceConfig: options.priceConfig || options.price_config || 'auth_price_per_month'
  };
}

function builtinPaymentAuth(options = {}) {
  return {
    type: 'builtin_payment',
    priceConfig: options.priceConfig || options.price_config || 'auth_price_per_month',
    pointsPerRMBConfig: options.pointsPerRMBConfig || options.points_per_rmb_config || 'auth_payment_points_per_rmb',
    defaultPointsPerRMB: Number(options.defaultPointsPerRMB || options.default_points_per_rmb || 100),
    timeoutConfig: options.timeoutConfig || options.timeout_config || 'auth_payment_timeout',
    timeout: Number(options.timeout || 300),
    methodsConfig: options.methodsConfig || options.methods_config || 'auth_payment_methods',
    methods: Array.isArray(options.methods) ? options.methods : []
  };
}

class AccountQLPlugin {
  constructor(options) {
    this.options = options;
    this.prefix = String(options.prefix || '').trim();
    this.tableName = String(options.tableName || options.table_name || '').trim();
    this.envName = String(options.envName || options.env_name || options.ql?.envName || options.ql?.env_name || '').trim();
    this.account = options.account || {};
    this.auth = options.auth || { provider: builtinPointsAuth() };
    this.ql = options.ql || {};
    this.schedules = options.schedules || {};
    this.routes = options.routes || {};
    if (!this.prefix) throw new Error('prefix 不能为空');
    if (!this.tableName) throw new Error('tableName 不能为空');
    if (!isValidEnvName(this.envName)) throw new Error('envName 格式无效');
  }

  run() {
    return runDirect((ctx) => this.handle(ctx));
  }

  store(ctx) {
    return new AccountStore(ctx, { tableName: this.tableName });
  }

  helpers(ctx) {
    const store = this.store(ctx);
    const helpers = {
      plugin: this,
      ctx,
      prefix: this.prefix,
      envName: this.envName,
      tableName: this.tableName,
      store,
      saveAccount: (input, extra = {}) => this.saveAccount(ctx, store, input, extra),
      listMine: (filter = {}) => store.listMine({ envName: this.envName, ...filter }),
      listAll: (filter = {}) => store.listAll({ envName: this.envName, ...filter }),
      accountName: (account) => this.accountName(account),
      formatAuthStatus,
      isAuthorized: (account) => isAccountAuthorized(accountExpiresAt(account)),
      authorize: (account) => this.authorizeAccount(ctx, account),
      runAccount: (account) => this.runSingleAccount(ctx, account),
      queryAccounts: (accounts, title, options = {}) => this.replyAccountQueries(ctx, accounts, title, options)
    };
    helpers.save_account = helpers.saveAccount;
    helpers.list_mine = helpers.listMine;
    helpers.list_all = helpers.listAll;
    helpers.account_name = helpers.accountName;
    helpers.format_auth_status = helpers.formatAuthStatus;
    helpers.is_authorized = helpers.isAuthorized;
    helpers.run_account = helpers.runAccount;
    helpers.query_accounts = helpers.queryAccounts;
    return helpers;
  }

  async handle(ctx) {
    await this.ensureSchedules(ctx);
    const content = String(ctx.content || '').trim();
    if (!content.startsWith(this.prefix)) return ctx.reply(this.helpText());
    const suffix = content.slice(this.prefix.length).trim() || '帮助';
    const helpers = this.helpers(ctx);
    if (this.routes[suffix]) return this.routes[suffix](ctx, helpers);
    if (suffix === '登录') return this.login(ctx, helpers);
    if (suffix === '账号' || suffix === '管理') return this.listAccounts(ctx, helpers);
    if (suffix === '查询') return this.queryMine(ctx, helpers);
    if (suffix === '运行') return this.runTask(ctx, false, helpers);
    if (suffix === '一键运行' || suffix === '签到') return this.runTask(ctx, true, helpers);
    if (suffix === '授权') return this.grantAuth(ctx, helpers);
    if (suffix === '删除') return this.deleteByMenu(ctx, helpers);
    if (suffix === 'CK检测') return this.checkCk(ctx, helpers);
    if (suffix === '过期检测') return this.checkExpirations(ctx, helpers);
    return ctx.reply(this.helpText());
  }

  async login(ctx, helpers) {
    if (typeof this.options.login === 'function') return this.options.login(ctx, helpers);
    if (typeof this.account.parseInput !== 'function') return ctx.reply('该插件没有配置登录处理');
    await ctx.reply(this.options.loginPrompt || `请发送${this.prefix}账号 CK，回复 q 退出：`);
    const raw = String(await ctx.listen(120)).trim();
    if (!raw || raw.toLowerCase() === 'q') return ctx.reply('已取消登录');
    const input = await this.account.parseInput(raw, ctx);
    const saved = await helpers.saveAccount(input);
    return ctx.reply(`✅${saved.existing ? '覆盖更新' : '添加'}成功：${saved.account.account_name}\n${saved.existingExpiresAt ? `已保留授权到期时间：${formatTime(saved.existingExpiresAt)}\n` : ''}发送【${this.prefix}授权】授权后即可运行。`);
  }

  async saveAccount(ctx, store, input, extra = {}) {
    const normalized = this.normalizeInput(input, ctx);
    const existing = await this.findExistingAccount(store, normalized);
    const metadata = {
      ...((existing && existing.metadata) || {}),
      ...(normalized.metadata || {}),
      account_key: normalized.uniqueKey,
      updated_at: new Date().toISOString(),
      ...(extra.metadata || {})
    };
    const account = await store.save({
      id: existing ? existing.id : 0,
      unionId: ctx.unionId,
      platform: ctx.platform,
      userId: ctx.userId,
      accountName: existing?.account_name || normalized.displayName,
      envName: this.envName,
      envValue: normalized.envValue,
      remark: normalized.remark || existing?.remark || '',
      status: existing?.status || 'active',
      metadata,
      expiresAt: existing?.expires_at || ''
    });
    return { account, existing: Boolean(existing), existingAccount: existing || null, existingExpiresAt: existing?.expires_at || '' };
  }

  normalizeInput(input, ctx) {
    const value = typeof input === 'string' ? { envValue: input } : { ...(input || {}) };
    const uniqueKey = String(value.uniqueKey || value.unique_key || (typeof this.account.uniqueKey === 'function' ? this.account.uniqueKey(value, ctx) : '')).trim();
    const envValue = String(value.envValue || value.env_value || value.value || '').trim();
    const displayName = String(value.displayName || value.display_name || value.accountName || value.account_name || (typeof this.account.displayName === 'function' ? this.account.displayName(value, ctx) : '') || uniqueKey || envValue.slice(0, 8)).trim();
    if (!uniqueKey) throw new Error('账号唯一键不能为空');
    if (!envValue) throw new Error('账号 CK 不能为空');
    validateEnvValue(envValue, Boolean(this.account.allowMultiline || this.account.allow_multiline));
    return { ...value, uniqueKey, envValue, displayName, remark: String(value.remark || '').trim(), metadata: value.metadata || {} };
  }

  async findExistingAccount(store, input) {
    const accounts = await store.listMine({ envName: this.envName });
    return accounts.find((account) => account.metadata?.account_key === input.uniqueKey || String(account.env_value || '') === input.envValue);
  }

  async listAccounts(ctx, helpers) {
    const accounts = await helpers.listMine();
    if (!accounts.length) return ctx.reply(`暂无账号，请发送【${this.prefix}登录】添加。`);
    const lines = accounts.map((item, index) => `${index + 1}. ${this.accountName(item)}｜${formatAuthStatus(accountExpiresAt(item))}｜${maskValue(item.env_value)}｜${item.status || 'active'}`);
    await this.sendButtons(ctx, `=====${this.prefix}账号管理=====\n${lines.join('\n')}\n------------------\n回复序号可操作账号，回复 q 退出：`, this.accountChoiceButtons(accounts));
    const choice = String(await ctx.listen(60)).trim().toLowerCase();
    if (choice === 'q') return ctx.reply('已退出账号管理');
    const account = accounts[Number(choice) - 1];
    if (!account) return ctx.reply('❌账号序号错误');
    await this.sendButtons(ctx, `当前账号：${this.accountName(account)}\n[1] 授权账号\n[2] 删除账号\n[3] 运行当前账号\n回复 q 退出：`, [
      [this.userButton(ctx, '授权账号', '1'), this.userButton(ctx, '运行当前账号', '3')],
      [this.userButton(ctx, '删除账号', '2')],
      [this.userButton(ctx, '退出', 'q')]
    ]);
    const action = String(await ctx.listen(60)).trim().toLowerCase();
    if (action === '1') return this.authorizeAccount(ctx, account);
    if (action === '2') return this.deleteAccount(ctx, account);
    if (action === '3') return this.runSingleAccount(ctx, account);
    return ctx.reply('已退出账号管理');
  }

  async deleteByMenu(ctx, helpers) {
    const accounts = await helpers.listMine();
    if (!accounts.length) return ctx.reply('暂无账号可删除。');
    await this.sendButtons(ctx, `请选择要删除的账号：\n${accounts.map((item, index) => `${index + 1}. ${this.accountName(item)}`).join('\n')}\n回复 q 退出：`, this.accountChoiceButtons(accounts));
    const choice = String(await ctx.listen(60)).trim().toLowerCase();
    if (choice === 'q') return ctx.reply('已取消删除');
    const account = accounts[Number(choice) - 1];
    if (!account) return ctx.reply('❌账号序号错误');
    return this.deleteAccount(ctx, account);
  }

  async deleteAccount(ctx, account) {
    await this.sendButtons(ctx, `确认删除账号【${this.accountName(account)}】吗？回复 y 确认：`, [[this.userButton(ctx, '确认删除', 'y')], [this.userButton(ctx, '取消', 'q')]]);
    const confirm = String(await ctx.listen(30)).trim().toLowerCase();
    if (confirm !== 'y') return ctx.reply('已取消删除');
    await this.store(ctx).delete(account.id);
    return ctx.reply(`✅已删除账号：${this.accountName(account)}`);
  }

  async grantAuth(ctx, helpers) {
    const accounts = await helpers.listMine();
    if (!accounts.length) return ctx.reply(`暂无账号，请先发送【${this.prefix}登录】添加。`);
    if (accounts.length === 1) return this.authorizeAccount(ctx, accounts[0]);
    await this.sendButtons(ctx, `请选择要授权的账号：\n${accounts.map((item, index) => `${index + 1}. ${this.accountName(item)}｜${formatAuthStatus(accountExpiresAt(item))}`).join('\n')}`, this.accountChoiceButtons(accounts));
    const choice = Number(String(await ctx.listen(60)).trim());
    const account = accounts[choice - 1];
    if (!account) return ctx.reply('❌账号序号错误');
    return this.authorizeAccount(ctx, account);
  }

  async authorizeAccount(ctx, account) {
    const provider = this.auth.provider || builtinPointsAuth();
    const months = await this.askMonths(ctx, provider, account);
    if (!months) return;
    let expiresAt;
    try {
      expiresAt = typeof provider.authorize === 'function'
        ? await provider.authorize(ctx, account, months)
        : provider.type === 'builtin_payment'
          ? await authorizeByBuiltinPayment(ctx, account, months, provider, this)
          : await authorizeByBuiltinPoints(ctx, account, months, provider, this);
    } catch (error) {
      return ctx.reply(error.message || '授权失败');
    }
    await this.store(ctx).save({
      id: account.id,
      unionId: account.union_id,
      platform: account.platform,
      userId: account.user_id,
      accountName: account.account_name,
      envName: account.env_name,
      envValue: account.env_value,
      remark: account.remark,
      status: account.status || 'active',
      metadata: { ...(account.metadata || {}), auth_source: provider.type || 'custom' },
      expiresAt
    });
    return ctx.reply(`✅授权成功：${this.accountName(account)}，到期时间 ${formatTime(expiresAt)}`);
  }

  async askMonths(ctx, provider, account) {
    const quote = typeof provider.quote === 'function' ? await provider.quote(ctx, account) : builtinQuote(ctx, provider);
    await this.sendButtons(ctx, `请输入授权月数（必须大于 0）${quote.description ? `（${quote.description}）` : ''}：`, [[this.userButton(ctx, '1个月', '1'), this.userButton(ctx, '3个月', '3'), this.userButton(ctx, '12个月', '12')], [this.userButton(ctx, '取消', 'q')]]);
    const months = Number(String(await ctx.listen(60)).trim() || '0');
    if (!Number.isFinite(months) || months <= 0) {
      await ctx.reply('❌授权月数必须大于 0');
      return 0;
    }
    return months;
  }

  async queryMine(ctx, helpers) {
    const accounts = await helpers.listMine({ status: 'active' });
    if (!accounts.length) return ctx.reply(`暂无账号，请发送【${this.prefix}登录】添加。`);
    if (accounts.length === 1) return this.replyAccountQueries(ctx, accounts, `${this.prefix}账号查询结果：`, { separate: false });
    const selected = await this.selectAccountsForQuery(ctx, accounts);
    if (!selected) return ctx.reply('已取消查询');
    return this.replyAccountQueries(ctx, selected.accounts, `${this.prefix}账号查询结果：`, { separate: selected.all });
  }

  supportsButtons(ctx) {
    return ctx && ['telegram', 'qq_office'].includes(ctx.platform) && typeof ctx.sendButtons === 'function';
  }

  buildQuerySelectionButtons(pageAccounts, start, totalPages, page, userId = '') {
    const restrictUserId = String(userId || '').trim();
    const button = (text, value) => {
      const item = { text, value };
      if (restrictUserId) item.user_id = restrictUserId;
      return item;
    };
    const rows = [[button('[0] 全部查询', '0')]];
    for (let index = 0; index < pageAccounts.length; index += 4) {
      rows.push(pageAccounts.slice(index, index + 4).map((account, offset) => {
        const number = start + index + offset + 1;
        return button(`[${number}] ${this.accountName(account)}`, String(number));
      }));
    }
    if (totalPages > 1) {
      rows.push([
        button('上一页', 'p'),
        button(`第 ${page + 1}/${totalPages} 页`, 'page'),
        button('下一页', 'n')
      ]);
      rows.push([button('退出', 'q')]);
    } else {
      rows.push([button('退出', 'q')]);
    }
    return rows;
  }

  async sendQuerySelectionPrompt(ctx, text, buttons) {
    if (this.supportsButtons(ctx)) {
      await ctx.sendButtons(text, buttons);
      return;
    }
    await ctx.reply(text);
  }

  async selectAccountsForQuery(ctx, accounts) {
    const pageSize = 20;
    const totalPages = Math.ceil(accounts.length / pageSize);
    let page = 0;
    while (true) {
      const start = page * pageSize;
      const pageAccounts = accounts.slice(start, start + pageSize);
      const lines = ['请输入要查询的账号：', '[0] 全部查询', '--------------------'];
      pageAccounts.forEach((account, index) => lines.push(`[${start + index + 1}] ${this.accountName(account)}`));
      if (totalPages > 1) lines.push('--------------------', `第 ${page + 1}/${totalPages} 页，回复 n 下一页，p 上一页，q 退出`);
      await this.sendQuerySelectionPrompt(ctx, lines.join('\n'), this.buildQuerySelectionButtons(pageAccounts, start, totalPages, page, ctx.userId || ctx.user_id));
      const choice = String(await ctx.listen(60)).trim().toLowerCase();
      if (!choice || choice === 'q') return null;
      if (choice === '0') return { all: true, accounts };
      if (choice === 'page') {
        continue;
      }
      if (totalPages > 1 && choice === 'n') {
        page = (page + 1) % totalPages;
        continue;
      }
      if (totalPages > 1 && choice === 'p') {
        page = (page - 1 + totalPages) % totalPages;
        continue;
      }
      const account = accounts[Number(choice) - 1];
      if (!account) {
        await ctx.reply('❌账号序号错误');
        return null;
      }
      return { all: false, accounts: [account] };
    }
  }

  async sendButtons(ctx, text, buttons) {
    if (typeof ctx.sendButtons === 'function') return ctx.sendButtons(text, buttons);
    return ctx.reply(text);
  }

  userButton(ctx, text, value) {
    const button = { text, value };
    const userId = ctx.userId || ctx.user_id || '';
    if (userId) button.user_id = userId;
    return button;
  }

  accountChoiceButtons(accounts) {
    const rows = [];
    for (let index = 0; index < accounts.length; index += 4) {
      rows.push(accounts.slice(index, index + 4).map((account, offset) => ({ text: `${index + offset + 1}. ${short(this.accountName(account), 12)}`, value: String(index + offset + 1) })));
    }
    rows.push([{ text: '退出', value: 'q' }]);
    return rows;
  }

  async replyAccountQueries(ctx, accounts, title, options = {}) {
    if (typeof this.account.query !== 'function') return ctx.reply(`${title}\n该插件没有配置查询功能。`);
    const separate = options.separate !== false;
    if (separate) {
      await ctx.reply(title);
      for (let index = 0; index < accounts.length; index++) {
        await ctx.reply(await this.queryAccountText(ctx, accounts[index], index));
      }
      return;
    }
    const lines = [];
    for (let index = 0; index < accounts.length; index++) {
      lines.push(await this.queryAccountText(ctx, accounts[index], index));
    }
    return ctx.reply(`${title}\n${lines.join('\n')}`);
  }

  async queryAccountText(ctx, account, index) {
    try {
      const result = await this.account.query(account, ctx, index);
      return formatQueryResult(this.accountName(account), result, account);
    } catch (error) {
      return `${this.accountName(account)}｜查询失败：${error.message}｜到期：${formatAuthStatus(accountExpiresAt(account))}`;
    }
  }

  async runTask(ctx, allUsers, helpers) {
    if (allUsers && !ctx.isAdmin() && ctx.meta('fake') !== 'true') return ctx.reply(`❌${this.prefix}一键运行仅平台管理员或定时任务可用，用户请发送【${this.prefix}运行】。`);
    const accounts = allUsers ? await helpers.listAll({ status: 'active' }) : await helpers.listMine({ status: 'active' });
    if (!accounts.length) return ctx.reply(`暂无可运行账号，请先发送【${this.prefix}登录】添加。`);
    const runnableAccounts = accounts.filter((account) => isAccountAuthorized(accountExpiresAt(account)));
    if (!runnableAccounts.length) return ctx.reply(`暂无已授权账号，请先发送【${this.prefix}授权】选择账号授权。`);
    return this.runAccounts(ctx, runnableAccounts, allUsers ? 'all_authorized' : 'current_user', allUsers ? `${this.prefix}一键运行` : `${this.prefix}运行`);
  }

  async runSingleAccount(ctx, account) {
    if (!isAccountAuthorized(accountExpiresAt(account))) return ctx.reply(`❌${this.accountName(account)}未授权或已过期，请先授权。`);
    return this.runAccounts(ctx, [account], 'single_account', `${this.prefix}账号运行：${this.accountName(account)}`);
  }

  async runAccounts(ctx, accounts, runMode, title) {
    const timeoutConfig = this.ql.timeoutConfig || this.ql.timeout_config || 'run_wait_timeout';
    const timeout = Number(ctx.config(timeoutConfig, this.ql.timeout || 7200) || this.ql.timeout || 7200);
    const isScheduled = ctx.meta('fake') === 'true';
    const waitScheduled = this.ql.waitScheduled ?? this.ql.wait_scheduled;
    const wait = isScheduled ? waitScheduled === true : true;
    if (wait && !isScheduled) await ctx.reply(`🚀开始执行${title}，共 ${accounts.length} 个账号。`);
    const startedAt = Date.now();
    const runtimeConfig = this.ql.runtimeConfig || this.ql.runtime_config || 'script_runtime';
    const scriptConfig = this.ql.scriptConfig || this.ql.script_config || 'task_script';
    const result = await ctx.runQLScript({
      runtime: String(ctx.config(runtimeConfig, this.ql.runtime || 'nodejs') || this.ql.runtime || 'nodejs'),
      script: String(ctx.config(scriptConfig, this.ql.script || '') || this.ql.script || ''),
      envName: this.envName,
      accounts,
      runMode,
      timeout,
      wait,
      env: typeof this.ql.env === 'function' ? this.ql.env(ctx, accounts) : (this.ql.env || {})
    });
    if (!wait) return ctx.reply(scriptTaskMessage(result, `${title}任务已提交`));
    if (result?.already_running && result?.status === 'running') {
      if (runMode !== 'all_authorized' || ctx.config("notify_scheduled") === "true") return ctx.reply(scriptTaskMessage(result, `${title}任务正在运行`));
      return;
    }
    if (result?.timeout) {
      if (runMode !== 'all_authorized' || ctx.config("notify_scheduled") === "true") return ctx.reply(scriptTaskMessage(result, `${title}仍在运行`));
      return;
    }
    if (result?.status !== 'success') {
      if (runMode !== 'all_authorized' || ctx.config("notify_scheduled") === "true") return ctx.reply(this.runSummaryMessage(result, accounts.length, Date.now() - startedAt));
      return;
    }
    if (runMode === 'all_authorized') await this.callAfterRun(ctx, accounts, result, runMode, title, isScheduled);
    await this.checkCkAfterRun(ctx, accounts);
    if (runMode !== 'all_authorized' && typeof this.account.query === 'function') return this.replyAccountQueries(ctx, accounts, '运行后账号信息：', { separate: true });
    if (runMode === 'all_authorized') {
      if (ctx.config("notify_scheduled") === "true") return ctx.reply(this.runSummaryMessage(result, accounts.length, Date.now() - startedAt));
    } else {
      return ctx.reply(`✅执行完成：${title}`);
    }
  }

  runSummaryMessage(result, accountCount, elapsedMs) {
    if (result?.status !== 'success') return `❌${this.prefix}生活运行失败！共运行${accountCount}个账号，耗时${formatDurationSeconds(elapsedMs)}${result?.error ? `，错误：${result.error}` : ''}`;
    return `✅${this.prefix}生活运行完成！共运行${accountCount}个账号，耗时${formatDurationSeconds(elapsedMs)}`;
  }

  async callAfterRun(ctx, accounts, result, runMode, title, isScheduled) {
    const afterRun = this.ql.afterRun || this.ql.after_run;
    if (typeof afterRun !== 'function') return;
    const helpers = this.helpers(ctx);
    helpers.runMode = runMode;
    helpers.run_mode = runMode;
    helpers.title = title;
    helpers.isScheduled = isScheduled;
    helpers.is_scheduled = isScheduled;
    try {
      await afterRun(ctx, accounts, result, helpers);
    } catch (error) {
      console.log(`${this.prefix} afterRun 执行失败：${error?.message || error}`);
    }
  }

  async checkCk(ctx, helpers) {
    if (!ctx.isAdmin() && ctx.meta('fake') !== 'true') return ctx.reply(`❌${this.prefix}CK检测仅平台管理员或定时任务可用。`);
    const result = await this.scanCk(ctx, await helpers.listAll({ status: 'active' }));
    return ctx.reply(`✅${this.prefix}CK检测完成：账号 ${result.accounts} 个，正常 ${result.valid} 个，失效 ${result.invalid} 个，通知 ${result.notified} 个，异常 ${result.errors} 个。`);
  }

  async checkCkAfterRun(ctx, accounts) {
    if (typeof this.account.checkCk !== 'function') return;
    try {
      const result = await this.scanCk(ctx, accounts);
      if (result.invalid > 0) console.log(`${this.prefix}CK检测：发现 ${result.invalid} 个失效账号，已通知 ${result.notified} 个。`);
    } catch (error) {
      console.log(`${this.prefix}运行后CK检测失败：${error.message}`);
    }
  }

  async scanCk(ctx, accounts) {
    if (typeof this.account.checkCk !== 'function') throw new Error('该插件没有配置 CK 检测功能');
    return this.store(ctx).scanCkStatus({
      accounts,
      title: `${this.prefix} CK`,
      checker: (account) => this.account.checkCk(account, ctx),
      message: (account, state) => `【${this.prefix}CK提醒】${this.accountName(account)} ${state.reason || 'CK 已失效'}，请发送【${this.prefix}登录】重新登录或更新 CK。`
    });
  }

  async checkExpirations(ctx, helpers) {
    if (!ctx.isAdmin() && ctx.meta('fake') !== 'true') return ctx.reply(`❌${this.prefix}过期检测仅平台管理员或定时任务可用。`);
    const notifyDays = parseDays(ctx.config('expire_notify_days', '7,3,1,0'));
    const deleteAfterDays = Number(ctx.config('expire_delete_after_days', -1));
    const result = await this.store(ctx).scanExpirations({
      envName: this.envName,
      notifyDays,
      deleteAfterDays,
      title: `${this.prefix}账号授权`,
      unauthorizedMessage: (account, state) => `【${this.prefix}账号授权提醒】${this.accountName(account)} 尚未授权${state.daysSinceCreated !== null ? `（已添加 ${state.daysSinceCreated} 天）` : ''}，请发送【${this.prefix}账号】完成授权。`,
      message: (account, state) => expirationMessage(this.prefix, this.accountName(account), state)
    });
    return ctx.reply(`✅${this.prefix}过期检测完成：账号 ${result.accounts} 个，通知 ${result.notified} 个，删除 ${result.deleted} 个，跳过 ${result.skipped} 个。`);
  }

  async ensureSchedules(ctx) {
    let admins = [];
    try {
      admins = await ctx.listPlatformAdmins();
    } catch (error) {
      console.log(`声明${this.prefix}定时任务失败：获取管理员身份失败：${error.message}`);
      return;
    }
    const admin = admins[0];
    if (!admin) {
      console.log(`声明${this.prefix}定时任务失败：没有已启动平台的管理员身份`);
      return;
    }
    const normalizedSchedules = normalizeSchedules(this.prefix, this.schedules);
    const defaultMaxCount = Math.max(3, normalizedSchedules.length);
    for (const schedule of normalizedSchedules) {
      try {
        await ctx.setScheduledTask({
          taskKey: schedule.taskKey,
          name: schedule.name,
          description: schedule.description,
          cron: String(ctx.config(schedule.cronConfig, schedule.cron) || schedule.cron),
          platform: admin.platform,
          adapterId: admin.adapter_id,
          userId: admin.user_id,
          groupId: '',
          content: schedule.content,
          maxCount: Math.max(schedule.maxCount || 0, defaultMaxCount)
        });
      } catch (error) {
        console.log(`声明${schedule.name}失败：${error.message}`);
      }
    }
  }

  accountName(account) {
    return account?.account_name || account?.accountName || account?.remark || String(account?.env_value || '').slice(0, 8) || '未知账号';
  }

  helpText() {
    const list = [`${this.prefix}登录`, `${this.prefix}账号`, `${this.prefix}查询`, `${this.prefix}运行`, `${this.prefix}一键运行`, `${this.prefix}授权`, `${this.prefix}删除`];
    if (typeof this.account.checkCk === 'function') list.push(`${this.prefix}CK检测`);
    if (this.schedules.expireCheck || this.schedules.expire_check) list.push(`${this.prefix}过期检测`);
    for (const command of Object.keys(this.routes)) list.push(`${this.prefix}${command}`);
    return `支持指令：${list.join(' / ')}`;
  }
}

class AccountStore {
  constructor(ctx, options = {}) {
    this.ctx = ctx;
    this.tableName = String(options.tableName || options.table_name || '');
  }

  async save(account = {}) {
    return this.ctx._request({
      action: 'account_save',
      table_name: String(account.tableName || account.table_name || this.tableName || ''),
      id: Number(account.id || 0),
      union_id: String(account.unionId || account.union_id || this.ctx.unionId || ''),
      platform: String(account.platform || this.ctx.platform || ''),
      user_id: String(account.userId || account.user_id || this.ctx.userId || ''),
      account_name: String(account.accountName || account.account_name || account.name || ''),
      env_name: String(account.envName || account.env_name || ''),
      env_value: String(account.envValue || account.env_value || account.value || ''),
      remark: String(account.remark || ''),
      status: String(account.status || 'active'),
      metadata: account.metadata || {},
      expires_at: normalizeDateTime(account.expiresAt || account.expires_at || '')
    }, 'account_response');
  }

  listMine(options = {}) { return this.list({ ...options, scope: 'mine' }); }
  listAll(options = {}) { return this.list({ ...options, scope: 'all' }); }

  async list(options = {}) {
    return this.ctx._request({
      action: 'account_list',
      table_name: String(options.tableName || options.table_name || this.tableName || ''),
      scope: String(options.scope || 'mine'),
      union_id: String(options.unionId || options.union_id || this.ctx.unionId || ''),
      env_name: String(options.envName || options.env_name || ''),
      status: String(options.status || '')
    }, 'account_response');
  }

  async delete(id, options = {}) {
    await this.ctx._request({
      action: 'account_delete',
      table_name: String(options.tableName || options.table_name || this.tableName || ''),
      scope: String(options.scope || 'mine'),
      id: Number(id || 0),
      union_id: String(options.unionId || options.union_id || this.ctx.unionId || '')
    }, 'account_response');
    return true;
  }

  async scanExpirations(options = {}) {
    const now = Date.now();
    const notifyDays = parseDays(options.notifyDays ?? options.notify_days ?? [7, 3, 1, 0]);
    const deleteAfterDays = Number(options.deleteAfterDays ?? options.delete_after_days ?? -1);
    const accounts = await this.listAll({ envName: options.envName || options.env_name || '', status: options.status || 'active' });
    const result = { notified: 0, deleted: 0, skipped: 0, accounts: accounts.length };
    for (const account of accounts) {
      const expiresAt = accountExpiresAt(account);
      if (!expiresAt) {
        const createdTime = new Date(account.created_at || account.createdAt || '').getTime();
        const unauthorizedExpired = deleteAfterDays >= 0 && Number.isFinite(createdTime) && now - createdTime >= deleteAfterDays * 86400000;
        if (options.notify !== false) {
          const text = typeof options.unauthorizedMessage === 'function' ? options.unauthorizedMessage(account, { daysSinceCreated: Number.isFinite(createdTime) ? Math.floor((now - createdTime) / 86400000) : null }) : `【${options.title || '账号授权'}提醒】${account.account_name || '账号'} 尚未授权。`;
          if (await this.notifyAccount(account, text)) result.notified++;
        } else if (!unauthorizedExpired) {
          result.skipped++;
        }
        if (unauthorizedExpired && options.deleteExpired !== false && options.delete_expired !== false) {
          await this.delete(account.id, { scope: 'all', unionId: account.union_id || account.unionId });
          result.deleted++;
        }
        continue;
      }
      const expiresTime = new Date(expiresAt).getTime();
      if (!Number.isFinite(expiresTime)) { result.skipped++; continue; }
      const daysLeft = Math.ceil((expiresTime - now) / 86400000);
      const notifyMatched = notifyDays.includes(daysLeft) || (daysLeft < 0 && notifyDays.includes(0));
      const shouldDelete = deleteAfterDays >= 0 && now - expiresTime >= deleteAfterDays * 86400000;
      if ((notifyMatched || shouldDelete) && options.notify !== false) {
        const text = typeof options.message === 'function' ? options.message(account, { daysLeft, expiresAt }) : `${options.title || '账号授权'} ${daysLeft >= 0 ? `将在 ${daysLeft} 天后过期` : '已过期'}`;
        if (await this.notifyAccount(account, text)) result.notified++;
      }
      if (shouldDelete && options.deleteExpired !== false && options.delete_expired !== false) {
        await this.delete(account.id, { scope: 'all', unionId: account.union_id || account.unionId });
        result.deleted++;
      }
    }
    return result;
  }

  async scanCkStatus(options = {}) {
    const checker = options.checker || options.check;
    if (typeof checker !== 'function') throw new Error('scanCkStatus 需要传入 checker(account) 函数');
    const accounts = Array.isArray(options.accounts) ? options.accounts : await this.listAll({ envName: options.envName || options.env_name || '', status: options.status || 'active' });
    const result = { accounts: accounts.length, checked: 0, valid: 0, invalid: 0, notified: 0, skipped: 0, errors: 0 };
    for (const account of accounts) {
      if (!account || typeof account !== 'object') { result.skipped++; continue; }
      try {
        const state = await checker(account);
        result.checked++;
        const valid = typeof state === 'boolean' ? state : Boolean(state && state.valid);
        if (valid) { result.valid++; continue; }
        result.invalid++;
        if (options.notify === false) continue;
        const text = typeof options.message === 'function' ? options.message(account, state || {}) : `【${options.title || '账号 CK'}提醒】${account.account_name || '账号'} CK 已失效。`;
        if (await this.notifyAccount(account, text)) result.notified++;
      } catch (error) {
        result.errors++;
        result.skipped++;
      }
    }
    return result;
  }

  async notifyAccount(account, text) {
    try {
      await this.ctx.sendMessage({
        unionId: account.union_id || account.unionId || '',
        platform: account.platform || this.ctx.platform,
        userId: account.user_id || account.userId,
        text
      });
      return true;
    } catch (error) {
      return false;
    }
  }
}

function builtinQuote(ctx, provider) {
  const price = Math.max(0, Number(ctx.config(provider.priceConfig || 'auth_price_per_month', 0) || 0));
  const unit = ctx.pointsUnit || ctx.points_unit || '积分';
  return { price, unit, description: price > 0 ? `${price}${unit}/月` : '免费' };
}

async function authorizeByBuiltinPoints(ctx, account, months, provider, plugin) {
  const quote = builtinQuote(ctx, provider);
  const totalCost = quote.price * months;
  if (totalCost > 0) {
    await plugin.sendButtons(ctx, `本次授权需要扣除 ${totalCost}${quote.unit}，当前 ${ctx.points}${quote.unit}，回复 y 确认：`, [[plugin.userButton(ctx, '确认授权', 'y')], [plugin.userButton(ctx, '取消', 'q')]]);
    const confirm = String(await ctx.listen(30)).trim().toLowerCase();
    if (confirm !== 'y') throw new Error('已取消授权');
    await ctx.consumePoints(totalCost);
  }
  const baseTime = Math.max(Date.now(), new Date(accountExpiresAt(account)).getTime() || 0);
  return new Date(baseTime + months * 30 * 86400000);
}

async function authorizeByBuiltinPayment(ctx, account, months, provider, plugin) {
  const quote = builtinQuote(ctx, provider);
  const totalPoints = quote.price * months;
  if (totalPoints > 0) {
    const pointsPerRMB = paymentPointsPerRMB(ctx, provider);
    const amountRMB = formatRMB(totalPoints / pointsPerRMB);
    const timeout = Number(ctx.config(provider.timeoutConfig, provider.timeout) || provider.timeout || 300);
    const result = await new PAY(ctx).waitPay(`${plugin.prefix}账号授权：${plugin.accountName(account)} × ${months}个月`, amountRMB, timeout, {
      methods: paymentMethods(ctx, provider),
      metadata: {
        plugin: plugin.prefix,
        account_id: account.id,
        account_name: plugin.accountName(account),
        months,
        auth_points: totalPoints,
        points_per_rmb: pointsPerRMB
      },
      remark: `${plugin.prefix}账号授权 ${months} 个月`
    });
    if (!result || result.status !== 'paid') {
      throw new Error(result?.message || '支付未完成，授权失败');
    }
  }
  const baseTime = Math.max(Date.now(), new Date(accountExpiresAt(account)).getTime() || 0);
  return new Date(baseTime + months * 30 * 86400000);
}

function paymentPointsPerRMB(ctx, provider) {
  const value = Number(ctx.config(provider.pointsPerRMBConfig, provider.defaultPointsPerRMB) || provider.defaultPointsPerRMB || 100);
  return Number.isFinite(value) && value > 0 ? value : 100;
}

function paymentMethods(ctx, provider) {
  const configured = String(ctx.config(provider.methodsConfig, '') || '').split(',').map((item) => item.trim()).filter(Boolean);
  if (configured.length > 0) return configured;
  return provider.methods || [];
}

function formatRMB(value) {
  return Math.ceil(Number(value || 0) * 100) / 100;
}

function normalizeSchedules(prefix, schedules) {
  const list = [];
  const normalizeItem = (item, defaults) => ({
    taskKey: item.taskKey || item.task_key || defaults.taskKey,
    name: item.name || defaults.name,
    description: item.description || defaults.description,
    cronConfig: item.cronConfig || item.cron_config || defaults.cronConfig,
    cron: item.cron || defaults.cron,
    content: item.content || defaults.content,
    maxCount: item.maxCount || item.max_count || defaults.maxCount
  });
  const runItems = [];
  if (schedules.run) runItems.push(schedules.run);
  const extraRuns = schedules.runs || schedules.runList || schedules.run_list || [];
  if (Array.isArray(extraRuns)) runItems.push(...extraRuns);
  for (const item of runItems) {
    list.push(normalizeItem(item, { taskKey: `${prefix}-default-run`, name: `${prefix}自动运行`, description: '插件触发一次后自动声明默认运行任务', cronConfig: 'cron', cron: '0 8 * * *', content: `${prefix}一键运行`, maxCount: 3 }));
  }
  const expireCheck = schedules.expireCheck || schedules.expire_check;
  if (expireCheck) list.push(normalizeItem(expireCheck, { taskKey: `${prefix}-expiration-check`, name: `${prefix}过期检测`, description: '检测账号授权到期并提醒续费', cronConfig: 'expire_check_cron', cron: '15 9 * * *', content: `${prefix}过期检测`, maxCount: 3 }));
  const ckCheck = schedules.ckCheck || schedules.ck_check;
  if (ckCheck) list.push(normalizeItem(ckCheck, { taskKey: `${prefix}-ck-check`, name: `${prefix} CK 检测`, description: '检测账号 CK 是否失效，仅通知用户，不删除账号', cronConfig: 'ck_check_cron', cron: '25 9 * * *', content: `${prefix}CK检测`, maxCount: 3 }));
  return list;
}

function formatQueryResult(name, result, account) {
  if (typeof result === 'string') return result;
  if (!result || typeof result !== 'object') return `${name}｜到期：${formatAuthStatus(accountExpiresAt(account))}`;
  const pairs = Object.entries(result).map(([key, value]) => `${key}：${value}`);
  return `${name}｜${pairs.join('｜')}｜到期：${formatAuthStatus(accountExpiresAt(account))}`;
}

function accountExpiresAt(account) { return account?.expires_at || account?.expiresAt || ''; }
function isAccountAuthorized(expiresAt) { const time = new Date(expiresAt || '').getTime(); return Number.isFinite(time) && time > Date.now(); }
function formatAuthStatus(expiresAt) { if (!expiresAt) return '未授权'; return isAccountAuthorized(expiresAt) ? `${formatTime(expiresAt)}` : `已过期`; }
function formatTime(value) { const date = value instanceof Date ? value : new Date(value); return Number.isFinite(date.getTime()) ? date.toLocaleString('zh-CN', { hour12: false }) : String(value || '无'); }
function formatDurationSeconds(ms) { const seconds = Math.max(0, Number(ms) || 0) / 1000; return `${seconds.toFixed(3)}秒`; }
function maskValue(value) { const text = String(value || ''); return text.length <= 12 ? (text ? `${text.slice(0, 4)}****` : '空') : `${text.slice(0, 6)}****${text.slice(-4)}`; }
function short(value, max) { const text = String(value || ''); return text.length > max ? `${text.slice(0, max)}…` : text; }
function parseDays(value) { return (Array.isArray(value) ? value : String(value || '').split(',')).map((item) => Number(String(item).trim())).filter((item) => Number.isFinite(item)); }
function normalizeDateTime(value) { if (!value) return ''; const date = value instanceof Date ? value : new Date(value); return Number.isFinite(date.getTime()) ? date.toISOString() : String(value); }
function isValidEnvName(name) { return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name); }
function validateEnvValue(value, allowMultiline) { if (value.includes('\0')) throw new Error('账号 CK 不能包含空字符'); if (!allowMultiline && /[\r\n]/.test(value)) throw new Error('单个账号 CK 不能包含换行'); }
function expirationMessage(prefix, name, state) { if (state.daysLeft > 0) return `【${prefix}账号授权提醒】${name} 将在 ${state.daysLeft} 天后过期，请发送【${prefix}账号】续费。`; if (state.daysLeft === 0) return `【${prefix}账号授权提醒】${name} 今天到期，请发送【${prefix}账号】续费。`; return `【${prefix}账号授权提醒】${name} 已过期，请发送【${prefix}账号】续费后继续使用。`; }
function scriptTaskMessage(result, fallback) { const id = result?.task_id || result?.log_id || result?.id || ''; const status = result?.already_running ? '任务已在运行' : '任务已创建'; return `✅${fallback || status}${id ? `\n任务ID：${id}` : ''}\n请到后台【脚本任务】查看运行状态和日志。`; }

module.exports = { createAccountQLPlugin, builtinPointsAuth, builtinPaymentAuth, AccountQLPlugin, AccountStore, normalizeSchedules };
