package builtin

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"strings"
	"time"

	"github.com/allbot/allbot/core/types"
)

type RestartRequest struct {
	MessageKey string
	Platform   string
	AdapterID  string
	UserID     string
	GroupID    string
	Target     string
	StartedAt  time.Time
}

func replyRestart(ctx *Context) error {
	if ctx.ReserveRestart == nil {
		return ctx.SendText("重启功能未初始化")
	}
	handler, alreadyRequested := ctx.ReserveRestart()
	if handler == nil {
		return ctx.SendText("重启功能未初始化")
	}
	if alreadyRequested {
		return ctx.SendText("重启已在执行")
	}
	messageKey := RestartMessageKey(ctx.Message)
	if ctx.MessageKey != nil {
		messageKey = ctx.MessageKey(ctx.Message)
	}
	request := RestartRequest{
		MessageKey: messageKey,
		Platform:   ctx.Message.Platform,
		AdapterID:  ctx.adapterID(),
		UserID:     ctx.Message.UserID,
		GroupID:    ctx.Message.GroupID,
		Target:     ctx.Target,
		StartedAt:  time.Now(),
	}
	if err := ctx.SendText("已收到重启指令，AllBot 正在重启"); err != nil {
		log.Printf("[WARN][重启] 确认消息发送失败，仍继续重启: %v", err)
	}
	go func() {
		if err := handler(request); err != nil {
			if ctx.ReleaseRestart != nil {
				ctx.ReleaseRestart()
			}
			_ = ctx.SendText(err.Error())
		}
	}()
	return nil
}

func RestartMessageKey(msg *types.Message) string {
	if msg == nil {
		return ""
	}
	adapterID := msg.AdapterID
	if adapterID == "" && msg.Metadata != nil {
		adapterID = msg.Metadata["adapter_id"]
	}
	parts := []string{msg.Platform, adapterID, msg.UserID, msg.GroupID, msg.ID, msg.Content}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(digest[:])
}
