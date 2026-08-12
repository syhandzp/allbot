package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/allbot/allbot/core/config"
	"github.com/allbot/allbot/core/deps"
	"github.com/allbot/allbot/core/types"
)

type Manager struct {
	plugins               map[string]*PluginProcess
	runningScripts        map[int64]context.CancelFunc
	scriptDone            map[int64]chan ScriptRunResult
	queuedScripts         map[int64]*queuedScriptRun
	queueWake             chan struct{}
	schedulerOnce         sync.Once
	mu                    sync.RWMutex
	pluginDir             string
	depsManager           *deps.Manager
	database              *config.Database
	scriptLimit           int
	scriptTimeoutNotifier ScriptTimeoutNotifier
}

type PluginProcess struct {
	Plugin *types.Plugin
	Cmd    *exec.Cmd
	Port   int
	Status string
}

type PluginDBAction struct {
	Action    string
	RequestID string
	Table     string
	Columns   []config.PluginTableColumn
	Values    map[string]interface{}
	RowID     int64
	Query     config.PluginDBQuery
}

type PluginDBResult struct {
	Success bool
	Error   string
	Data    interface{}
}

type PluginUserResult struct {
	Success bool
	Error   string
	Data    interface{}
}

type FakeMessageAction struct {
	Platform  string
	AdapterID string
	UserID    string
	GroupID   string
	Content   string
}

type SendMessageAction struct {
	Platform  string
	AdapterID string
	UserID    string
	GroupID   string
	UnionID   string
	Text      string
	Buttons   [][]types.ButtonOption
}

type RichMessageAction struct {
	Platform     string
	AdapterID    string
	UserID       string
	GroupID      string
	UnionID      string
	Parts        []types.RichMessagePart
	FallbackText string
	Prefer       string
}

type PluginWebResponse struct {
	Status  int
	Headers map[string]string
	Body    string
	JSON    interface{}
	Data    map[string]interface{}
}

type PluginConfigAction struct {
	AccessControl types.AccessControlConfig
}

type ScheduledTaskAction struct {
	TaskKey     string
	Name        string
	Description string
	Enabled     bool
	Pinned      bool
	Cron        string
	Platform    string
	AdapterID   string
	UserID      string
	GroupID     string
	Content     string
	MaxCount    int
}

type PluginAccountAction struct {
	Action      string
	RequestID   string
	TableName   string
	Scope       string
	ID          int64
	UnionID     string
	Platform    string
	UserID      string
	AccountName string
	EnvName     string
	EnvValue    string
	Remark      string
	Status      string
	Metadata    map[string]interface{}
	ExpiresAt   string
}

type ScriptRunAction struct {
	RequestID      string
	PluginID       string
	Runtime        string
	RuntimeProfile string
	Script         string
	Cwd            string
	Env            map[string]string
	Timeout        int
	Wait           bool
	RunMode        string
	UnionID        string
}

type ScriptRunResult struct {
	Status     string    `json:"status"`
	Output     string    `json:"output"`
	Error      string    `json:"error"`
	FinishedAt time.Time `json:"finished_at"`
}

type ScriptTimeoutNotification struct {
	LogID          int64
	TimeoutSeconds int
	Output         string
	Error          string
	FinishedAt     time.Time
}

type ScriptTimeoutNotifier func(ScriptTimeoutNotification)

type queuedScriptRun struct {
	logID             int64
	pluginPath        string
	runtimeName       string
	fullScript        string
	workDir           string
	resolved          deps.ResolvedRuntime
	action            ScriptRunAction
	done              chan ScriptRunResult
	startedAt         time.Time
	createdAt         time.Time
	runTimeoutSeconds int
}

type ScriptRunTask struct {
	ID             int64     `json:"id"`
	PluginID       string    `json:"plugin_id"`
	UnionID        string    `json:"union_id"`
	ScriptPath     string    `json:"script_path"`
	Runtime        string    `json:"runtime"`
	RuntimeProfile string    `json:"runtime_profile"`
	RunMode        string    `json:"run_mode"`
	Status         string    `json:"status"`
	StartedAt      time.Time `json:"started_at"`
}

type PluginAuthorizationAction struct {
	Action    string
	RequestID string
	TableName string
	UnionID   string
	Amount    int64
	Status    string
	Plan      string
	Source    string
	Metadata  map[string]interface{}
	ExpiresAt string
}

type PaymentWaitAction struct {
	RequestID string
	Subject   string
	AmountRaw json.RawMessage
	Timeout   int
	UnionID   string
	Methods   []string
	Metadata  map[string]interface{}
	Remark    string
}

func NewManager(pluginDir string, depsManager *deps.Manager) *Manager {
	return &Manager{plugins: make(map[string]*PluginProcess), runningScripts: make(map[int64]context.CancelFunc), scriptDone: make(map[int64]chan ScriptRunResult), pluginDir: pluginDir, depsManager: depsManager, scriptLimit: 1}
}

func (m *Manager) SetScriptTimeoutNotifier(notifier ScriptTimeoutNotifier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scriptTimeoutNotifier = notifier
}

func (m *Manager) PluginDir() string {
	if m == nil {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pluginDir
}

func (m *Manager) SetDatabase(database *config.Database) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.database = database
	if database != nil {
		if limit, err := database.GetSetting("script_tasks.concurrent_limit"); err == nil {
			if parsed, parseErr := strconv.Atoi(strings.TrimSpace(limit)); parseErr == nil && parsed > 0 {
				m.scriptLimit = parsed
			}
		}
	}
}

func (m *Manager) SetScriptLimit(limit int) {
	if limit <= 0 {
		limit = 1
	}
	m.mu.Lock()
	m.scriptLimit = limit
	m.mu.Unlock()
	m.signalScriptQueue()
}

func (m *Manager) GetDepsManager() *deps.Manager {
	return m.depsManager
}

func (m *Manager) PluginPath(pluginID string) string {
	return filepath.Join(m.pluginDir, pluginID)
}

func (m *Manager) LoadPlugin(pluginPath string) (*types.Plugin, error) {
	plugin, err := m.loadPluginConfig(pluginPath)
	if err != nil {
		return nil, err
	}

	m.installDeps(plugin)

	m.mu.Lock()
	m.plugins[plugin.ID] = &PluginProcess{Plugin: plugin, Status: "ready"}
	m.mu.Unlock()
	return plugin, nil
}

func (m *Manager) LoadAllPlugins() ([]*types.Plugin, error) {
	entries, err := os.ReadDir(m.pluginDir)
	if err != nil {
		return nil, err
	}

	plugins := make([]*types.Plugin, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pluginPath := filepath.Join(m.pluginDir, entry.Name())
		plugin, err := m.LoadPlugin(pluginPath)
		if err != nil {
			log.Printf("[SYSTEM] 加载插件失败 %s: %v", entry.Name(), err)
			continue
		}
		plugins = append(plugins, plugin)
	}
	m.restoreQueuedScriptRuns()
	return plugins, nil
}

func (m *Manager) GetPlugin(pluginID string) *PluginProcess {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.plugins[pluginID]
}

func (m *Manager) GetAllPlugins() []*PluginProcess {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*PluginProcess, 0, len(m.plugins))
	for _, process := range m.plugins {
		result = append(result, process)
	}
	return result
}

func (m *Manager) TogglePlugin(pluginID string, enabled bool) error {
	m.mu.Lock()
	process, ok := m.plugins[pluginID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("plugin not found: %s", pluginID)
	}

	if err := m.updatePluginConfigValue(pluginID, "enabled", enabled); err != nil {
		return err
	}

	process.Plugin.Enabled = enabled
	return nil
}

func (m *Manager) SetPluginPinned(pluginID string, pinned bool) error {
	m.mu.Lock()
	process, ok := m.plugins[pluginID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("plugin not found: %s", pluginID)
	}

	if err := m.updatePluginConfigValue(pluginID, "pinned", pinned); err != nil {
		return err
	}

	process.Plugin.Pinned = pinned
	return nil
}

func (m *Manager) updatePluginConfigValue(pluginID, key string, value interface{}) error {
	configPath := filepath.Join(m.pluginDir, pluginID, "plugin.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	var config map[string]interface{}
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	config[key] = value
	newData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, newData, 0644)
}

func (m *Manager) ReloadPlugin(pluginID string) error {
	pluginPath := filepath.Join(m.pluginDir, pluginID)
	plugin, err := m.loadPluginConfig(pluginPath)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if process, ok := m.plugins[pluginID]; ok {
		process.Plugin = plugin
		process.Status = "ready"
	} else {
		m.plugins[pluginID] = &PluginProcess{Plugin: plugin, Status: "ready"}
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) SavePluginAccessControl(pluginID string, accessControl types.AccessControlConfig) error {
	configPath := filepath.Join(m.pluginDir, pluginID, "plugin.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	raw["access_control"] = accessControl
	updated, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(configPath, updated, 0644); err != nil {
		return err
	}
	return m.ReloadPlugin(pluginID)
}

func (m *Manager) StopPlugin(pluginID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	process, ok := m.plugins[pluginID]
	if !ok || process.Cmd == nil || process.Cmd.Process == nil {
		return nil
	}
	_ = process.Cmd.Process.Kill()
	process.Cmd = nil
	process.Status = "ready"
	return nil
}

func (m *Manager) StartPluginByID(pluginID string) error {
	return m.ReloadPlugin(pluginID)
}

const (
	pluginOutputMaxLineBytes = 4 * 1024 * 1024
	pluginLogMergeWindow     = 30 * time.Millisecond
)

func newPluginOutputScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), pluginOutputMaxLineBytes)
	return scanner
}

type pluginProcessLogBatch struct {
	scope  string
	id     string
	stream string
	lines  []string
}

func newPluginProcessLogBatch(scope string, id string, stream string) *pluginProcessLogBatch {
	return &pluginProcessLogBatch{scope: scope, id: id, stream: stream}
}

func (b *pluginProcessLogBatch) Add(line string) {
	if line == "" {
		return
	}
	b.lines = append(b.lines, line)
}

func (b *pluginProcessLogBatch) Flush() {
	if len(b.lines) == 0 {
		return
	}
	log.Printf("[SYSTEM][%s][%s][%s] %s", b.scope, b.id, b.stream, strings.Join(b.lines, "\n"))
	b.lines = nil
}

func isPluginProtocolAction(action string) bool {
	switch action {
	case "reply", "send_markdown", "send_rich", "send_buttons", "send_image", "send_file", "listen", "set_data_view", "db_create_table", "db_query", "db_insert", "db_update", "db_delete", "db_clear", "fake_message", "send_message", "send_rich_message", "get_union_id", "list_platform_admins", "set_access_control", "set_scheduled_task", "account_save", "account_list", "account_delete", "auth_check", "auth_grant", "auth_revoke", "points_consume", "points_add", "payment_wait", "run_script", "web_response", "done":
		return true
	default:
		return false
	}
}

func isOpenAPIProtocolAction(action string) bool {
	switch action {
	case "http_response", "db_create_table", "db_query", "db_insert", "db_update", "db_delete", "db_clear", "send_message", "done":
		return true
	default:
		return false
	}
}

func isClosedPipeScanError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "file already closed") || strings.Contains(message, "closed pipe")
}

func scanPluginProcessStderr(reader io.Reader, scope string, id string) {
	lineCh := make(chan string, 16)
	errCh := make(chan error, 1)
	go func() {
		scanner := newPluginOutputScanner(reader)
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
		errCh <- scanner.Err()
		close(lineCh)
	}()

	batch := newPluginProcessLogBatch(scope, id, "STDERR")
	var timer *time.Timer
	var timerCh <-chan time.Time
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerCh = nil
	}
	scheduleFlush := func() {
		if timer == nil {
			timer = time.NewTimer(pluginLogMergeWindow)
			timerCh = timer.C
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(pluginLogMergeWindow)
	}
	flush := func() {
		stopTimer()
		batch.Flush()
	}

	for {
		select {
		case line, ok := <-lineCh:
			if !ok {
				flush()
				if err := <-errCh; err != nil && !isClosedPipeScanError(err) {
					log.Printf("[SYSTEM][%s][%s][STDERR] scan error: %v", scope, id, err)
				}
				return
			}
			if line == "" {
				continue
			}
			if strings.HasPrefix(line, "[PLUGIN_LOG]") && len(batch.lines) > 0 {
				flush()
			}
			batch.Add(line)
			scheduleFlush()
		case <-timerCh:
			flush()
		}
	}
}

func waitPluginProcess(cmd *exec.Cmd, stderrDone <-chan struct{}) error {
	err := cmd.Wait()
	<-stderrDone
	return err
}

func (m *Manager) ExecutePlugin(plugin *types.Plugin, pluginPath string, messageJSON []byte, replyFunc func(string) error, imageFunc func(string) error, fileFunc func(string) error, listenFunc func(timeout int) string, dataViewFunc func(config.DataViewConfig) error, dbFunc func(pluginID string, action PluginDBAction) PluginDBResult, fakeMessageFunc func(pluginID string, action FakeMessageAction) error, sendMessageFunc func(pluginID string, action SendMessageAction) PluginUserResult, userFunc func() PluginUserResult, adminFunc func(platform string) PluginUserResult, configFunc func(pluginID string, action PluginConfigAction) PluginUserResult, scheduleFunc func(pluginID string, action ScheduledTaskAction) PluginUserResult, accountFunc func(pluginID string, action PluginAccountAction) PluginUserResult, authFunc func(pluginID string, action PluginAuthorizationAction) PluginUserResult, scriptFunc func(pluginID string, action ScriptRunAction) PluginUserResult, paymentFunc func(pluginID string, action PaymentWaitAction) PluginUserResult, callbacks ...interface{}) error {
	var replyMarkdownFunc func(string) error
	var replyRichFunc func(types.RichMessage) error
	var sendRichMessageFunc func(string, RichMessageAction) PluginUserResult
	var sendButtonsFunc func(string, [][]types.ButtonOption) error
	for _, callback := range callbacks {
		switch fn := callback.(type) {
		case func(string) error:
			if replyMarkdownFunc == nil {
				replyMarkdownFunc = fn
			}
		case func(types.RichMessage) error:
			if replyRichFunc == nil {
				replyRichFunc = fn
			}
		case func(string, RichMessageAction) PluginUserResult:
			if sendRichMessageFunc == nil {
				sendRichMessageFunc = fn
			}
		case func(string, [][]types.ButtonOption) error:
			if sendButtonsFunc == nil {
				sendButtonsFunc = fn
			}
		}
	}
	cmd, err := m.newDirectCommand(plugin, pluginPath)
	if err != nil {
		return err
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		scanPluginProcessStderr(stderr, "PLUGIN", plugin.ID)
	}()

	messageJSON = append(messageJSON, '\n')
	if _, err := stdin.Write(messageJSON); err != nil {
		_ = cmd.Process.Kill()
		_ = waitPluginProcess(cmd, stderrDone)
		return err
	}

	stdoutBatch := newPluginProcessLogBatch("PLUGIN", plugin.ID, "STDOUT")
	scanner := newPluginOutputScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var action struct {
			Action         string                     `json:"action"`
			RequestID      string                     `json:"request_id"`
			Text           string                     `json:"text"`
			Markdown       string                     `json:"markdown"`
			Buttons        [][]types.ButtonOption     `json:"buttons"`
			Parts          []types.RichMessagePart    `json:"parts"`
			FallbackText   string                     `json:"fallback_text"`
			Prefer         string                     `json:"prefer"`
			URL            string                     `json:"url"`
			Path           string                     `json:"path"`
			Timeout        int                        `json:"timeout"`
			Success        bool                       `json:"success"`
			Error          string                     `json:"error"`
			TableName      string                     `json:"table_name"`
			ViewName       string                     `json:"view_name"`
			GroupName      string                     `json:"group_name"`
			Description    string                     `json:"description"`
			Columns        []string                   `json:"columns"`
			DBTable        string                     `json:"table"`
			DBColumns      []config.PluginTableColumn `json:"db_columns"`
			Values         map[string]interface{}     `json:"values"`
			RowID          int64                      `json:"row_id"`
			Query          config.PluginDBQuery       `json:"query"`
			AccessControl  types.AccessControlConfig  `json:"access_control"`
			TaskKey        string                     `json:"task_key"`
			Name           string                     `json:"name"`
			Subject        string                     `json:"subject"`
			Enabled        bool                       `json:"enabled"`
			Pinned         bool                       `json:"pinned"`
			MaxCount       int                        `json:"max_count"`
			Platform       string                     `json:"platform"`
			AdapterID      string                     `json:"adapter_id"`
			UserID         string                     `json:"user_id"`
			GroupID        string                     `json:"group_id"`
			Content        string                     `json:"content"`
			TextMessage    string                     `json:"text_message"`
			Cron           string                     `json:"cron"`
			Scope          string                     `json:"scope"`
			ID             int64                      `json:"id"`
			UnionID        string                     `json:"union_id"`
			AccountName    string                     `json:"account_name"`
			EnvName        string                     `json:"env_name"`
			EnvValue       string                     `json:"env_value"`
			Remark         string                     `json:"remark"`
			Status         string                     `json:"status"`
			Plan           string                     `json:"plan"`
			Source         string                     `json:"source"`
			Amount         json.RawMessage            `json:"amount"`
			AmountRMB      json.RawMessage            `json:"amount_rmb"`
			Metadata       map[string]interface{}     `json:"metadata"`
			Runtime        string                     `json:"runtime"`
			RuntimeProfile string                     `json:"runtime_profile"`
			Script         string                     `json:"script"`
			Cwd            string                     `json:"cwd"`
			Env            map[string]string          `json:"env"`
			Methods        []string                   `json:"methods"`
			Wait           bool                       `json:"wait"`
			RunMode        string                     `json:"run_mode"`
			ExpiresAt      string                     `json:"expires_at"`
		}
		if err := json.Unmarshal([]byte(line), &action); err != nil {
			stdoutBatch.Add(line)
			continue
		}
		if action.Action == "" || !isPluginProtocolAction(action.Action) {
			stdoutBatch.Add(line)
			continue
		}
		stdoutBatch.Flush()

		switch action.Action {
		case "reply":
			if replyFunc != nil && action.Text != "" {
				text := action.Text
				go func() { _ = replyFunc(text) }()
			}
		case "send_markdown":
			if replyMarkdownFunc != nil {
				markdown := action.Markdown
				if markdown == "" {
					markdown = action.Content
				}
				if markdown == "" {
					markdown = action.Text
				}
				if markdown != "" {
					go func() { _ = replyMarkdownFunc(markdown) }()
				}
			}
		case "send_rich":
			if replyRichFunc != nil {
				message := types.RichMessage{Parts: action.Parts, FallbackText: action.FallbackText, Prefer: action.Prefer}
				if len(message.Parts) > 0 || message.FallbackText != "" {
					go func() { _ = replyRichFunc(message) }()
				}
			}
		case "send_buttons":
			if sendButtonsFunc != nil {
				text := action.Text
				if text == "" {
					text = action.Content
				}
				if text == "" {
					text = action.Markdown
				}
				if text != "" {
					go func() { _ = sendButtonsFunc(text, action.Buttons) }()
				}
			}
		case "send_image":
			if imageFunc != nil && action.URL != "" {
				url := action.URL
				go func() {
					if err := imageFunc(url); err != nil {
						log.Printf("[SYSTEM] Plugin %s send image failed: %v", plugin.ID, err)
					}
				}()
			}
		case "send_file":
			if fileFunc != nil && action.Path != "" {
				path := action.Path
				go func() {
					if err := fileFunc(path); err != nil {
						log.Printf("[SYSTEM] Plugin %s send file failed: %v", plugin.ID, err)
					}
				}()
			}
		case "listen":
			timeout := action.Timeout
			if timeout <= 0 {
				timeout = 60
			}
			content := ""
			if listenFunc != nil {
				content = listenFunc(timeout)
			}
			response, _ := json.Marshal(map[string]string{"action": "listen_response", "content": content})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "set_data_view":
			if dataViewFunc != nil && action.TableName != "" {
				view := config.DataViewConfig{PluginID: plugin.ID, TableName: action.TableName, ViewName: action.ViewName, GroupName: action.GroupName, Description: action.Description, Columns: action.Columns}
				if err := dataViewFunc(view); err != nil {
					log.Printf("[SYSTEM] Plugin %s set data view failed: %v", plugin.ID, err)
				}
			}
		case "db_create_table", "db_query", "db_insert", "db_update", "db_delete", "db_clear":
			result := PluginDBResult{Success: false, Error: "数据库执行器不可用"}
			if dbFunc != nil {
				result = dbFunc(plugin.ID, PluginDBAction{Action: action.Action, RequestID: action.RequestID, Table: action.DBTable, Columns: action.DBColumns, Values: action.Values, RowID: action.RowID, Query: action.Query})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "db_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "fake_message":
			responseData := map[string]interface{}{"action": "fake_message_response", "request_id": action.RequestID, "success": true, "error": ""}
			if fakeMessageFunc == nil {
				responseData["success"] = false
				responseData["error"] = "伪造消息执行器不可用"
			} else if err := fakeMessageFunc(plugin.ID, FakeMessageAction{Platform: action.Platform, AdapterID: action.AdapterID, UserID: action.UserID, GroupID: action.GroupID, Content: action.Content}); err != nil {
				responseData["success"] = false
				responseData["error"] = err.Error()
			}
			response, _ := json.Marshal(responseData)
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "send_message":
			result := PluginUserResult{Success: false, Error: "消息发送器不可用"}
			if sendMessageFunc != nil {
				text := action.Text
				if text == "" {
					text = action.TextMessage
				}
				if text == "" {
					text = action.Content
				}
				result = sendMessageFunc(plugin.ID, SendMessageAction{Platform: action.Platform, AdapterID: action.AdapterID, UserID: action.UserID, GroupID: action.GroupID, UnionID: action.UnionID, Text: text})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "send_message_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "send_rich_message":
			result := PluginUserResult{Success: false, Error: "富文本消息发送器不可用"}
			if sendRichMessageFunc != nil {
				result = sendRichMessageFunc(plugin.ID, RichMessageAction{Platform: action.Platform, AdapterID: action.AdapterID, UserID: action.UserID, GroupID: action.GroupID, UnionID: action.UnionID, Parts: action.Parts, FallbackText: action.FallbackText, Prefer: action.Prefer})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "send_rich_message_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "get_union_id":
			result := PluginUserResult{Success: false, Error: "用户身份执行器不可用"}
			if userFunc != nil {
				result = userFunc()
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "union_id_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "list_platform_admins":
			result := PluginUserResult{Success: false, Error: "管理员身份执行器不可用"}
			if adminFunc != nil {
				result = adminFunc(action.Platform)
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "platform_admins_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "set_access_control":
			result := PluginUserResult{Success: false, Error: "插件配置执行器不可用"}
			if configFunc != nil {
				result = configFunc(plugin.ID, PluginConfigAction{AccessControl: action.AccessControl})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "access_control_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "set_scheduled_task":
			result := PluginUserResult{Success: false, Error: "定时任务执行器不可用"}
			if scheduleFunc != nil {
				result = scheduleFunc(plugin.ID, ScheduledTaskAction{TaskKey: action.TaskKey, Name: action.Name, Description: action.Description, Enabled: action.Enabled, Pinned: action.Pinned, Cron: action.Cron, Platform: action.Platform, AdapterID: action.AdapterID, UserID: action.UserID, GroupID: action.GroupID, Content: action.Content, MaxCount: action.MaxCount})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "scheduled_task_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "account_save", "account_list", "account_delete":
			result := PluginUserResult{Success: false, Error: "账号执行器不可用"}
			if accountFunc != nil {
				result = accountFunc(plugin.ID, PluginAccountAction{Action: action.Action, RequestID: action.RequestID, TableName: action.TableName, Scope: action.Scope, ID: action.ID, UnionID: action.UnionID, Platform: action.Platform, UserID: action.UserID, AccountName: action.AccountName, EnvName: action.EnvName, EnvValue: action.EnvValue, Remark: action.Remark, Status: action.Status, Metadata: action.Metadata, ExpiresAt: action.ExpiresAt})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "account_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "auth_check", "auth_grant", "auth_revoke", "points_consume", "points_add":
			result := PluginUserResult{Success: false, Error: "授权执行器不可用"}
			amount, amountErr := parseInt64Raw(action.Amount)
			if amountErr != nil {
				result = PluginUserResult{Success: false, Error: amountErr.Error()}
			} else if authFunc != nil {
				result = authFunc(plugin.ID, PluginAuthorizationAction{Action: action.Action, RequestID: action.RequestID, TableName: action.TableName, UnionID: action.UnionID, Amount: amount, Status: action.Status, Plan: action.Plan, Source: action.Source, Metadata: action.Metadata, ExpiresAt: action.ExpiresAt})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "auth_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "payment_wait":
			result := PluginUserResult{Success: false, Error: "支付执行器不可用"}
			if paymentFunc != nil {
				subject := action.Subject
				if subject == "" {
					subject = action.Name
				}
				result = paymentFunc(plugin.ID, PaymentWaitAction{RequestID: action.RequestID, Subject: subject, AmountRaw: paymentAmountRaw(action.Amount, action.AmountRMB), Timeout: action.Timeout, UnionID: action.UnionID, Methods: action.Methods, Metadata: action.Metadata, Remark: action.Remark})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "payment_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "run_script":
			result := PluginUserResult{Success: false, Error: "脚本执行器不可用"}
			if scriptFunc != nil {
				runtimeName := normalizePluginRuntime(action.Runtime)
				runtimeProfile := strings.TrimSpace(action.RuntimeProfile)
				if runtimeProfile == "" && (runtimeName == "" || runtimeName == normalizePluginRuntime(plugin.Runtime)) {
					runtimeProfile = strings.TrimSpace(plugin.RuntimeProfile)
				}
				result = scriptFunc(plugin.ID, ScriptRunAction{RequestID: action.RequestID, PluginID: plugin.ID, Runtime: action.Runtime, RuntimeProfile: runtimeProfile, Script: action.Script, Cwd: action.Cwd, Env: action.Env, Timeout: action.Timeout, Wait: action.Wait, RunMode: action.RunMode, UnionID: action.UnionID})
			}
			response, _ := json.Marshal(map[string]interface{}{"action": "script_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			response = append(response, '\n')
			_, _ = stdin.Write(response)
		case "done":
			if !action.Success && action.Error != "" {
				log.Printf("[SYSTEM] Plugin %s error: %s", plugin.ID, action.Error)
			}
			_ = stdin.Close()
			_ = waitPluginProcess(cmd, stderrDone)
			return nil
		}
	}

	stdoutBatch.Flush()
	_ = stdin.Close()
	_ = waitPluginProcess(cmd, stderrDone)
	return scanner.Err()
}

func parseInt64Raw(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	var number int64
	if err := json.Unmarshal(raw, &number); err == nil {
		return number, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		text = strings.TrimSpace(text)
		var parsed int64
		if _, err := fmt.Sscanf(text, "%d", &parsed); err != nil || fmt.Sprintf("%d", parsed) != text {
			return 0, fmt.Errorf("积分数量必须是整数")
		}
		return parsed, nil
	}
	return 0, fmt.Errorf("积分数量必须是整数")
}

func paymentAmountRaw(actionAmount, amountRMB json.RawMessage) json.RawMessage {
	if len(actionAmount) > 0 && string(actionAmount) != "null" {
		return actionAmount
	}
	return amountRMB
}

func (m *Manager) ExecutePluginOpenAPI(plugin *types.Plugin, pluginPath string, request types.OpenAPIRequest) (types.OpenAPIResponse, error) {
	runtimeProfile := strings.TrimSpace(plugin.OpenAPI.RuntimeProfile)
	if runtimeProfile == "" {
		runtimeProfile = plugin.RuntimeProfile
	}
	endpoint := types.OpenAPIEndpoint{ID: plugin.ID, Name: plugin.Name, Path: plugin.OpenAPI.Path, Method: plugin.OpenAPI.Method, Enabled: plugin.OpenAPI.Enabled, Token: plugin.OpenAPI.Token, Runtime: plugin.Runtime, RuntimeProfile: runtimeProfile, Entry: plugin.Entry}
	return m.ExecuteOpenAPI(endpoint, pluginPath, request, nil, nil)
}

func (m *Manager) ExecutePluginWeb(plugin *types.Plugin, pluginPath string, payload []byte, dbFunc func(string, PluginDBAction) PluginDBResult, sendMessageFunc func(string, SendMessageAction) PluginUserResult) (PluginWebResponse, error) {
	response, err := m.executePluginWebViaDirect(plugin, pluginPath, payload, dbFunc, sendMessageFunc)
	return response, err
}

func (m *Manager) executePluginWebViaDirect(plugin *types.Plugin, pluginPath string, payload []byte, dbFunc func(string, PluginDBAction) PluginDBResult, sendMessageFunc func(string, SendMessageAction) PluginUserResult) (PluginWebResponse, error) {
	cmd, err := m.newDirectCommand(plugin, pluginPath)
	if err != nil {
		return PluginWebResponse{}, err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return PluginWebResponse{}, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return PluginWebResponse{}, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return PluginWebResponse{}, err
	}
	if err := cmd.Start(); err != nil {
		return PluginWebResponse{}, err
	}
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		scanPluginProcessStderr(stderr, "WEBAPI", plugin.ID)
	}()
	if _, err := stdin.Write(append(payload, '\n')); err != nil {
		_ = cmd.Process.Kill()
		_ = waitPluginProcess(cmd, stderrDone)
		return PluginWebResponse{}, err
	}
	response := PluginWebResponse{Status: 200, Headers: map[string]string{"Content-Type": "application/json; charset=utf-8"}}
	stdoutBatch := newPluginProcessLogBatch("WEBAPI", plugin.ID, "STDOUT")
	scanner := newPluginOutputScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var action struct {
			Action    string                     `json:"action"`
			RequestID string                     `json:"request_id"`
			Status    int                        `json:"status"`
			Headers   map[string]string          `json:"headers"`
			Body      string                     `json:"body"`
			JSON      interface{}                `json:"json"`
			Data      map[string]interface{}     `json:"data"`
			Success   bool                       `json:"success"`
			Error     string                     `json:"error"`
			Platform  string                     `json:"platform"`
			AdapterID string                     `json:"adapter_id"`
			UserID    string                     `json:"user_id"`
			GroupID   string                     `json:"group_id"`
			UnionID   string                     `json:"union_id"`
			Text      string                     `json:"text"`
			Content   string                     `json:"content"`
			DBTable   string                     `json:"table"`
			DBColumns []config.PluginTableColumn `json:"db_columns"`
			Values    map[string]interface{}     `json:"values"`
			RowID     int64                      `json:"row_id"`
			Query     config.PluginDBQuery       `json:"query"`
		}
		if err := json.Unmarshal([]byte(line), &action); err != nil {
			stdoutBatch.Add(line)
			continue
		}
		if action.Action == "" || !isPluginProtocolAction(action.Action) {
			stdoutBatch.Add(line)
			continue
		}
		stdoutBatch.Flush()
		switch action.Action {
		case "web_response":
			if action.Status > 0 {
				response.Status = action.Status
			}
			if action.Headers != nil {
				response.Headers = action.Headers
			}
			response.Body = action.Body
			response.JSON = action.JSON
			response.Data = action.Data
			_ = stdin.Close()
			_ = waitPluginProcess(cmd, stderrDone)
			return response, nil
		case "db_create_table", "db_query", "db_insert", "db_update", "db_delete", "db_clear":
			result := PluginDBResult{Success: false, Error: "数据库执行器不可用"}
			if dbFunc != nil {
				result = dbFunc(plugin.ID, PluginDBAction{Action: action.Action, RequestID: action.RequestID, Table: action.DBTable, Columns: action.DBColumns, Values: action.Values, RowID: action.RowID, Query: action.Query})
			}
			reply, _ := json.Marshal(map[string]interface{}{"action": "db_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			reply = append(reply, '\n')
			_, _ = stdin.Write(reply)
		case "send_message":
			result := PluginUserResult{Success: false, Error: "消息发送器不可用"}
			if sendMessageFunc != nil {
				text := action.Text
				if text == "" {
					text = action.Content
				}
				result = sendMessageFunc(plugin.ID, SendMessageAction{Platform: action.Platform, AdapterID: action.AdapterID, UserID: action.UserID, GroupID: action.GroupID, UnionID: action.UnionID, Text: text})
			}
			reply, _ := json.Marshal(map[string]interface{}{"action": "send_message_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			reply = append(reply, '\n')
			_, _ = stdin.Write(reply)
		case "done":
			if !action.Success && action.Error != "" {
				_ = waitPluginProcess(cmd, stderrDone)
				return PluginWebResponse{}, fmt.Errorf("%s", action.Error)
			}
		}
	}
	stdoutBatch.Flush()
	if err := scanner.Err(); err != nil {
		_ = cmd.Process.Kill()
		_ = waitPluginProcess(cmd, stderrDone)
		return PluginWebResponse{}, err
	}
	_ = stdin.Close()
	_ = waitPluginProcess(cmd, stderrDone)
	return PluginWebResponse{}, fmt.Errorf("插件 Web API 未返回 web_response")
}

func (m *Manager) ExecuteOpenAPI(endpoint types.OpenAPIEndpoint, workDir string, request types.OpenAPIRequest, dbFunc func(string, PluginDBAction) PluginDBResult, sendMessageFunc func(string, SendMessageAction) PluginUserResult) (types.OpenAPIResponse, error) {
	maskedEndpoint := endpoint
	if maskedEndpoint.Token != "" {
		maskedEndpoint.Token = "***"
	}
	payload, err := json.Marshal(map[string]interface{}{
		"event":     "open_api_request",
		"plugin_id": endpoint.ID,
		"open_api":  maskedEndpoint,
		"request":   request,
	})
	if err != nil {
		return types.OpenAPIResponse{}, err
	}
	cmd, err := m.newOpenAPICommand(endpoint, workDir)
	if err != nil {
		return types.OpenAPIResponse{}, err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return types.OpenAPIResponse{}, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return types.OpenAPIResponse{}, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return types.OpenAPIResponse{}, err
	}
	if err := cmd.Start(); err != nil {
		return types.OpenAPIResponse{}, err
	}
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		scanPluginProcessStderr(stderr, "OPENAPI", endpoint.ID)
	}()
	if _, err := stdin.Write(append(payload, '\n')); err != nil {
		_ = cmd.Process.Kill()
		_ = waitPluginProcess(cmd, stderrDone)
		return types.OpenAPIResponse{}, err
	}

	response := types.OpenAPIResponse{Status: 200, Headers: map[string]string{"Content-Type": "application/json; charset=utf-8"}}
	stdoutBatch := newPluginProcessLogBatch("OPENAPI", endpoint.ID, "STDOUT")
	scanner := newPluginOutputScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var action struct {
			Action    string                     `json:"action"`
			RequestID string                     `json:"request_id"`
			Status    int                        `json:"status"`
			Headers   map[string]string          `json:"headers"`
			Body      string                     `json:"body"`
			JSON      interface{}                `json:"json"`
			Data      map[string]interface{}     `json:"data"`
			Success   bool                       `json:"success"`
			Error     string                     `json:"error"`
			Platform  string                     `json:"platform"`
			AdapterID string                     `json:"adapter_id"`
			UserID    string                     `json:"user_id"`
			GroupID   string                     `json:"group_id"`
			UnionID   string                     `json:"union_id"`
			Text      string                     `json:"text"`
			TextMsg   string                     `json:"text_message"`
			Content   string                     `json:"content"`
			DBTable   string                     `json:"table"`
			DBColumns []config.PluginTableColumn `json:"db_columns"`
			Values    map[string]interface{}     `json:"values"`
			RowID     int64                      `json:"row_id"`
			Query     config.PluginDBQuery       `json:"query"`
		}
		if err := json.Unmarshal([]byte(line), &action); err != nil {
			stdoutBatch.Add(line)
			continue
		}
		if action.Action == "" || !isOpenAPIProtocolAction(action.Action) {
			stdoutBatch.Add(line)
			continue
		}
		stdoutBatch.Flush()
		switch action.Action {
		case "http_response":
			if action.Status > 0 {
				response.Status = action.Status
			}
			if action.Headers != nil {
				response.Headers = action.Headers
			}
			response.Body = action.Body
			response.JSON = action.JSON
			response.Data = action.Data
			_ = stdin.Close()
			_ = waitPluginProcess(cmd, stderrDone)
			return response, nil
		case "db_create_table", "db_query", "db_insert", "db_update", "db_delete", "db_clear":
			result := PluginDBResult{Success: false, Error: "数据库执行器不可用"}
			if dbFunc != nil {
				result = dbFunc(endpoint.ID, PluginDBAction{Action: action.Action, RequestID: action.RequestID, Table: action.DBTable, Columns: action.DBColumns, Values: action.Values, RowID: action.RowID, Query: action.Query})
			}
			reply, _ := json.Marshal(map[string]interface{}{"action": "db_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			reply = append(reply, '\n')
			_, _ = stdin.Write(reply)
		case "send_message":
			result := PluginUserResult{Success: false, Error: "消息发送器不可用"}
			if sendMessageFunc != nil {
				text := action.Text
				if text == "" {
					text = action.TextMsg
				}
				if text == "" {
					text = action.Content
				}
				result = sendMessageFunc(endpoint.ID, SendMessageAction{Platform: action.Platform, AdapterID: action.AdapterID, UserID: action.UserID, GroupID: action.GroupID, UnionID: action.UnionID, Text: text})
			}
			reply, _ := json.Marshal(map[string]interface{}{"action": "send_message_response", "request_id": action.RequestID, "success": result.Success, "error": result.Error, "data": result.Data})
			reply = append(reply, '\n')
			_, _ = stdin.Write(reply)
		case "done":
			if !action.Success && action.Error != "" {
				_ = waitPluginProcess(cmd, stderrDone)
				return types.OpenAPIResponse{}, fmt.Errorf("%s", action.Error)
			}
		}
	}
	stdoutBatch.Flush()
	if err := scanner.Err(); err != nil {
		_ = cmd.Process.Kill()
		_ = waitPluginProcess(cmd, stderrDone)
		return types.OpenAPIResponse{}, err
	}
	_ = stdin.Close()
	_ = waitPluginProcess(cmd, stderrDone)
	return types.OpenAPIResponse{}, fmt.Errorf("Open API 未返回 http_response")
}

func (m *Manager) newOpenAPICommand(endpoint types.OpenAPIEndpoint, workDir string) (*exec.Cmd, error) {
	sdkRoot, err := filepath.Abs("sdk")
	if err != nil {
		return nil, err
	}
	resolved, err := m.depsManager.ResolveRuntime(endpoint.Runtime, endpoint.RuntimeProfile)
	if err != nil {
		return nil, err
	}
	switch endpoint.Runtime {
	case "python":
		cmd := exec.Command(resolved.Executable, "-u", filepath.Join(sdkRoot, "python", "allbot_direct.py"), "openapi", endpoint.Entry)
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(), fmt.Sprintf("ALLBOT_PLUGIN_ID=%s", endpoint.ID), fmt.Sprintf("ALLBOT_RUNTIME_PROFILE=%s", resolved.Profile.ID), "PYTHONUTF8=1", "PYTHONUNBUFFERED=1")
		return cmd, nil
	case "nodejs":
		cmd := exec.Command(resolved.Executable, filepath.Join(sdkRoot, "nodejs", "allbot_direct.js"), "openapi", endpoint.Entry)
		cmd.Dir = workDir
		cmd.Env = append(os.Environ(), fmt.Sprintf("ALLBOT_PLUGIN_ID=%s", endpoint.ID), fmt.Sprintf("ALLBOT_RUNTIME_PROFILE=%s", resolved.Profile.ID), fmt.Sprintf("NODE_PATH=%s", resolved.NodePath))
		return cmd, nil
	default:
		return nil, fmt.Errorf("不支持的运行时: %s", endpoint.Runtime)
	}
}

func (m *Manager) newDirectCommand(plugin *types.Plugin, pluginPath string) (*exec.Cmd, error) {
	entryPath, err := pluginEntryPath(pluginPath, plugin.Runtime, plugin.Entry)
	if err != nil {
		return nil, err
	}
	resolved, err := m.depsManager.ResolveRuntime(plugin.Runtime, plugin.RuntimeProfile)
	if err != nil {
		return nil, err
	}
	switch plugin.Runtime {
	case "python":
		cmd := exec.Command(resolved.Executable, "-u", entryPath)
		cmd.Dir = pluginPath
		cmd.Env = append(os.Environ(), fmt.Sprintf("ALLBOT_PLUGIN_ID=%s", plugin.ID), fmt.Sprintf("ALLBOT_RUNTIME_PROFILE=%s", resolved.Profile.ID), "PYTHONUTF8=1", "PYTHONUNBUFFERED=1")
		return cmd, nil
	case "nodejs":
		cmd := exec.Command(resolved.Executable, entryPath)
		cmd.Dir = pluginPath
		cmd.Env = append(os.Environ(), fmt.Sprintf("ALLBOT_PLUGIN_ID=%s", plugin.ID), fmt.Sprintf("ALLBOT_RUNTIME_PROFILE=%s", resolved.Profile.ID), fmt.Sprintf("NODE_PATH=%s", resolved.NodePath))
		return cmd, nil
	default:
		return nil, fmt.Errorf("不支持的运行时: %s", plugin.Runtime)
	}
}

func (m *Manager) RunPluginScript(pluginPath string, action ScriptRunAction) PluginUserResult {
	runtimeName, fullScript, workDir, err := m.preparePluginScript(pluginPath, action)
	if err != nil {
		return PluginUserResult{Success: false, Error: err.Error()}
	}
	m.mu.RLock()
	database := m.database
	scriptEnv := types.ScriptEnvConfig{}
	if process := m.plugins[action.PluginID]; process != nil && process.Plugin != nil {
		scriptEnv = normalizeScriptEnvConfig(process.Plugin.ScriptEnv)
	}
	m.mu.RUnlock()
	if database == nil {
		return PluginUserResult{Success: false, Error: "数据库未初始化，无法创建脚本任务"}
	}
	if scriptEnv.Enabled {
		env, err := database.ScriptEnvMap(scriptEnv.Names)
		if err != nil {
			return PluginUserResult{Success: false, Error: err.Error()}
		}
		action.Env = mergeScriptEnv(env, action.Env)
	}
	action.RuntimeProfile = strings.TrimSpace(action.RuntimeProfile)
	resolved, err := m.depsManager.ResolveRuntime(runtimeName, action.RuntimeProfile)
	if err != nil {
		return PluginUserResult{Success: false, Error: err.Error()}
	}
	actualProfile := resolved.Profile.ID
	scriptPath := filepath.ToSlash(action.Script)
	runMode := strings.TrimSpace(action.RunMode)
	if runMode == "" {
		runMode = "manual"
	}
	existingLog, err := database.FindLatestScriptRunLog(action.PluginID, scriptPath, runMode, action.UnionID, actualProfile)
	if err != nil {
		return PluginUserResult{Success: false, Error: err.Error()}
	}
	if existingLog != nil && isScriptRunActiveStatus(existingLog.Status) {
		data := map[string]interface{}{"log_id": existingLog.ID, "task_id": existingLog.ID, "status": existingLog.Status, "runtime": existingLog.Runtime, "runtime_profile": existingLog.RuntimeProfile, "script": existingLog.ScriptPath, "already_running": existingLog.Status != config.ScriptRunStatusQueued, "reused": true}
		if existingLog.Status == config.ScriptRunStatusQueued {
			data["queued"] = true
		}
		if action.Wait {
			return m.waitScriptRun(existingLog.ID, data, action.Timeout)
		}
		return PluginUserResult{Success: true, Data: data}
	}
	startedAt := time.Now()
	logID, reused, err := database.UpsertScriptRunLog(config.ScriptRunLog{PluginID: action.PluginID, UnionID: action.UnionID, ScriptPath: scriptPath, Runtime: runtimeName, RuntimeProfile: actualProfile, RunMode: runMode, Status: config.ScriptRunStatusQueued, StartedAt: startedAt})
	if err != nil {
		return PluginUserResult{Success: false, Error: err.Error()}
	}
	data := map[string]interface{}{"log_id": logID, "task_id": logID, "status": config.ScriptRunStatusQueued, "runtime": runtimeName, "runtime_profile": actualProfile, "script": scriptPath, "already_running": false, "reused": reused}
	m.enqueueScriptRun(logID, pluginPath, runtimeName, fullScript, workDir, resolved, action, startedAt)
	if action.Wait {
		return m.waitScriptRun(logID, data, action.Timeout)
	}
	data["queued"] = true
	return PluginUserResult{Success: true, Data: data}
}

func (m *Manager) waitScriptRun(logID int64, data map[string]interface{}, timeoutSeconds int) PluginUserResult {
	m.mu.RLock()
	done := m.scriptDone[logID]
	m.mu.RUnlock()
	if done == nil {
		return PluginUserResult{Success: true, Data: data}
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = 600
	}
	select {
	case result := <-done:
		data["status"] = result.Status
		data["output"] = result.Output
		data["error"] = result.Error
		data["finished_at"] = result.FinishedAt
		return PluginUserResult{Success: result.Status == "success", Error: result.Error, Data: data}
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		if m.isQueuedScriptRun(logID) {
			data["status"] = config.ScriptRunStatusQueued
		} else {
			data["status"] = "running"
		}
		data["timeout"] = true
		return PluginUserResult{Success: true, Data: data}
	}
}

func (m *Manager) enqueueScriptRun(logID int64, pluginPath, runtimeName, fullScript, workDir string, resolved deps.ResolvedRuntime, action ScriptRunAction, startedAt time.Time) bool {
	m.mu.Lock()
	if m.runningScripts == nil {
		m.runningScripts = make(map[int64]context.CancelFunc)
	}
	if m.queuedScripts == nil {
		m.queuedScripts = make(map[int64]*queuedScriptRun)
	}
	if _, ok := m.runningScripts[logID]; ok {
		m.mu.Unlock()
		return false
	}
	if _, ok := m.queuedScripts[logID]; ok {
		m.mu.Unlock()
		return false
	}
	if m.queueWake == nil {
		m.queueWake = make(chan struct{}, 1)
	}
	done := make(chan ScriptRunResult, 1)
	queueItem := &queuedScriptRun{logID: logID, pluginPath: pluginPath, runtimeName: runtimeName, fullScript: fullScript, workDir: workDir, resolved: resolved, action: action, done: done, startedAt: startedAt, createdAt: time.Now(), runTimeoutSeconds: m.scriptTaskRunTimeoutSecondsLocked()}
	m.queuedScripts[logID] = queueItem
	m.scriptDone[logID] = done
	m.mu.Unlock()
	m.ensureScriptScheduler()
	m.signalScriptQueue()
	return true
}

func (m *Manager) scriptTaskRunTimeoutSecondsLocked() int {
	if m.database == nil {
		return 0
	}
	settings, err := m.database.GetScriptTaskSettings()
	if err != nil || settings.RunTimeoutSeconds < 0 || settings.RunTimeoutSeconds > 86400 {
		return 0
	}
	return settings.RunTimeoutSeconds
}

func (m *Manager) scriptTaskRunTimeoutSeconds() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.scriptTaskRunTimeoutSecondsLocked()
}

func (m *Manager) ensureScriptScheduler() {
	m.schedulerOnce.Do(func() {
		go m.runScriptScheduler()
	})
}

func (m *Manager) signalScriptQueue() {
	m.mu.RLock()
	wake := m.queueWake
	m.mu.RUnlock()
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (m *Manager) runScriptScheduler() {
	for range m.scriptQueueSignal() {
		for {
			queueItem := m.nextQueuedScriptRun()
			if queueItem == nil {
				break
			}
			var ctx context.Context
			var cancel context.CancelFunc
			if queueItem.runTimeoutSeconds > 0 {
				ctx, cancel = context.WithTimeout(context.Background(), time.Duration(queueItem.runTimeoutSeconds)*time.Second)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			m.mu.Lock()
			if _, ok := m.queuedScripts[queueItem.logID]; !ok {
				m.mu.Unlock()
				cancel()
				continue
			}
			if _, ok := m.runningScripts[queueItem.logID]; ok {
				m.mu.Unlock()
				cancel()
				continue
			}
			m.runningScripts[queueItem.logID] = cancel
			m.scriptDone[queueItem.logID] = queueItem.done
			delete(m.queuedScripts, queueItem.logID)
			m.mu.Unlock()
			if err := m.markScriptRunRunning(queueItem.logID); err != nil {
				cancel()
				m.finishScriptRun(queueItem.logID, "failed", "", err.Error(), time.Now())
				continue
			}
			if ctx.Err() != nil {
				m.finishScriptRun(queueItem.logID, "paused", "", "脚本任务已暂停", time.Now())
				continue
			}
			go m.runPluginScriptTask(ctx, queueItem.logID, queueItem.runtimeName, queueItem.fullScript, queueItem.workDir, queueItem.resolved, queueItem.action, queueItem.runTimeoutSeconds)
		}
	}
}

func (m *Manager) scriptQueueSignal() <-chan struct{} {
	m.mu.Lock()
	if m.queueWake == nil {
		m.queueWake = make(chan struct{}, 1)
	}
	wake := m.queueWake
	m.mu.Unlock()
	return wake
}

func (m *Manager) nextQueuedScriptRun() *queuedScriptRun {
	m.mu.RLock()
	runningCount := len(m.runningScripts)
	limit := m.scriptLimit
	if limit <= 0 {
		limit = 1
	}
	if runningCount >= limit {
		m.mu.RUnlock()
		return nil
	}
	database := m.database
	m.mu.RUnlock()
	if database == nil {
		return nil
	}
	items, err := database.ListQueuedScriptRunLogs(500)
	if err != nil || len(items) == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, item := range items {
		if queued := m.queuedScripts[item.ID]; queued != nil {
			return queued
		}
	}
	return nil
}

func (m *Manager) preparePluginScript(pluginPath string, action ScriptRunAction) (string, string, string, error) {
	runtimeName := strings.TrimSpace(action.Runtime)
	if runtimeName == "" {
		if strings.EqualFold(filepath.Ext(action.Script), ".py") {
			runtimeName = "python"
		} else {
			runtimeName = "nodejs"
		}
	}
	if runtimeName == "node" {
		runtimeName = "nodejs"
	}
	if runtimeName == "py" || runtimeName == "python3" {
		runtimeName = "python"
	}
	if runtimeName != "nodejs" && runtimeName != "python" && runtimeName != "shell" {
		return "", "", "", fmt.Errorf("仅支持 nodejs/python/shell 脚本运行时")
	}
	fullScript, err := safeRelativePath(pluginPath, action.Script)
	if err != nil {
		return "", "", "", err
	}
	if info, err := os.Stat(fullScript); err != nil || info.IsDir() {
		return "", "", "", fmt.Errorf("脚本文件不存在或不是文件")
	}
	workDir := filepath.Dir(fullScript)
	if strings.TrimSpace(action.Cwd) != "" {
		workDir, err = safeRelativePath(pluginPath, action.Cwd)
		if err != nil {
			return "", "", "", err
		}
	}
	return runtimeName, fullScript, workDir, nil
}

func mergeScriptEnv(stored map[string]string, explicit map[string]string) map[string]string {
	if len(stored) == 0 && len(explicit) == 0 {
		return nil
	}
	merged := make(map[string]string, len(stored)+len(explicit))
	for key, value := range stored {
		merged[key] = value
	}
	for key, value := range explicit {
		merged[key] = value
	}
	return merged
}

func (m *Manager) runPluginScriptTask(ctx context.Context, logID int64, runtimeName, fullScript, workDir string, resolved deps.ResolvedRuntime, action ScriptRunAction, runTimeoutSeconds int) {
	cmdArgs := []string{fullScript}
	if runtimeName == "python" {
		cmdArgs = []string{"-u", fullScript}
	}
	if runtimeName == "shell" {
		cmdArgs = []string{fullScript}
	}
	cmd := exec.CommandContext(ctx, resolved.Executable, cmdArgs...)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(), "ALLBOT_SCRIPT_RUN=1", fmt.Sprintf("ALLBOT_RUNTIME_PROFILE=%s", resolved.Profile.ID))
	if runtimeName == "nodejs" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("NODE_PATH=%s", resolved.NodePath))
	} else if runtimeName == "python" {
		cmd.Env = append(cmd.Env, "PYTHONUTF8=1", "PYTHONUNBUFFERED=1")
	}
	for key, value := range action.Env {
		key = strings.TrimSpace(key)
		if key == "" || strings.ContainsAny(key, "=\x00") {
			continue
		}
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
	}
	output := &scriptOutputBuffer{}
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		m.finishScriptRun(logID, "failed", output.String(), err.Error(), time.Now())
		return
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var err error
	for {
		select {
		case err = <-done:
			goto finished
		case <-ticker.C:
			m.updateScriptRunOutput(logID, output.String())
		}
	}

finished:
	finishedAt := time.Now()
	outputText := output.String()
	status := "success"
	errorText := ""
	if err != nil {
		switch ctx.Err() {
		case context.DeadlineExceeded:
			status = "failed"
			errorText = fmt.Sprintf("脚本任务运行超过 %d 秒，已自动停止", runTimeoutSeconds)
		case context.Canceled:
			status = "paused"
			errorText = "脚本任务已暂停"
		default:
			status = "failed"
			errorText = err.Error()
		}
	}
	m.finishScriptRun(logID, status, outputText, errorText, finishedAt)
	if ctx.Err() == context.DeadlineExceeded {
		m.notifyScriptTaskTimeout(ScriptTimeoutNotification{LogID: logID, TimeoutSeconds: runTimeoutSeconds, Output: outputText, Error: errorText, FinishedAt: finishedAt})
	}
}

func (m *Manager) notifyScriptTaskTimeout(notification ScriptTimeoutNotification) {
	m.mu.RLock()
	notifier := m.scriptTimeoutNotifier
	m.mu.RUnlock()
	if notifier != nil {
		go notifier(notification)
	}
}

func (m *Manager) markScriptRunRunning(logID int64) error {
	m.mu.RLock()
	database := m.database
	m.mu.RUnlock()
	if database == nil {
		return nil
	}
	return database.UpdateScriptRunLog(logID, "running", "", "", time.Time{})
}

func (m *Manager) removeQueuedScriptRun(logID int64) {
	m.mu.Lock()
	if queued := m.queuedScripts[logID]; queued != nil {
		delete(m.queuedScripts, logID)
	}
	if done := m.scriptDone[logID]; done != nil {
		close(done)
		delete(m.scriptDone, logID)
	}
	m.mu.Unlock()
	m.signalScriptQueue()
}

func (m *Manager) isQueuedScriptRun(logID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.queuedScripts[logID]
	return ok
}

func isScriptRunActiveStatus(status string) bool {
	return status == config.ScriptRunStatusQueued || status == "running" || status == "pausing"
}

func (m *Manager) updateScriptRunOutput(logID int64, outputText string) {
	m.mu.RLock()
	database := m.database
	m.mu.RUnlock()
	if database != nil {
		_ = database.UpdateScriptRunLog(logID, "running", outputText, "", time.Time{})
	}
}

func (m *Manager) finishScriptRun(logID int64, status, outputText, errorText string, finishedAt time.Time) {
	m.mu.RLock()
	database := m.database
	m.mu.RUnlock()
	if database != nil {
		_ = database.UpdateScriptRunLog(logID, status, outputText, errorText, finishedAt)
	}
	m.mu.Lock()
	if done := m.scriptDone[logID]; done != nil {
		select {
		case done <- ScriptRunResult{Status: status, Output: outputText, Error: errorText, FinishedAt: finishedAt}:
		default:
		}
		close(done)
		delete(m.scriptDone, logID)
	}
	delete(m.runningScripts, logID)
	delete(m.queuedScripts, logID)
	m.mu.Unlock()
	m.signalScriptQueue()
}

type scriptOutputBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *scriptOutputBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, data...)
	return len(data), nil
}

func (b *scriptOutputBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}

func (m *Manager) PauseScriptRun(logID int64) bool {
	m.mu.Lock()
	cancel := m.runningScripts[logID]
	if _, queued := m.queuedScripts[logID]; queued {
		delete(m.queuedScripts, logID)
		m.mu.Unlock()
		m.finishScriptRun(logID, "paused", "", "脚本任务已暂停", time.Now())
		return true
	}
	m.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (m *Manager) restoreQueuedScriptRuns() {
	m.mu.RLock()
	database := m.database
	m.mu.RUnlock()
	if database == nil {
		return
	}
	items, err := database.ListQueuedScriptRunLogs(500)
	if err != nil {
		return
	}
	for _, item := range items {
		m.mu.RLock()
		if _, ok := m.queuedScripts[item.ID]; ok {
			m.mu.RUnlock()
			continue
		}
		m.mu.RUnlock()
		queuedItem, err := m.buildQueuedScriptRun(item)
		if err != nil {
			continue
		}
		m.mu.Lock()
		if m.queuedScripts == nil {
			m.queuedScripts = make(map[int64]*queuedScriptRun)
		}
		if m.scriptDone == nil {
			m.scriptDone = make(map[int64]chan ScriptRunResult)
		}
		if m.queueWake == nil {
			m.queueWake = make(chan struct{}, 1)
		}
		m.queuedScripts[item.ID] = queuedItem
		m.scriptDone[item.ID] = queuedItem.done
		m.mu.Unlock()
	}
	m.signalScriptQueue()
	m.ensureScriptScheduler()
}

func (m *Manager) buildQueuedScriptRun(item *config.ScriptRunLog) (*queuedScriptRun, error) {
	pluginPath := filepath.Join(m.pluginDir, item.PluginID)
	action := ScriptRunAction{
		PluginID:       item.PluginID,
		Runtime:        item.Runtime,
		RuntimeProfile: item.RuntimeProfile,
		Script:         item.ScriptPath,
		RunMode:        item.RunMode,
		UnionID:        item.UnionID,
	}
	m.mu.RLock()
	process := m.plugins[item.PluginID]
	database := m.database
	m.mu.RUnlock()
	if process != nil && process.Plugin != nil && process.Plugin.ScriptEnv.Enabled && database != nil {
		env, err := database.ScriptEnvMap(process.Plugin.ScriptEnv.Names)
		if err == nil {
			action.Env = mergeScriptEnv(env, action.Env)
		}
	}
	runtimeName, fullScript, workDir, err := m.preparePluginScript(pluginPath, action)
	if err != nil {
		return nil, err
	}
	resolved, err := m.depsManager.ResolveRuntime(runtimeName, action.RuntimeProfile)
	if err != nil {
		return nil, err
	}
	return &queuedScriptRun{
		logID:       item.ID,
		pluginPath:  pluginPath,
		runtimeName: runtimeName,
		fullScript:  fullScript,
		workDir:     workDir,
		resolved:    resolved,
		action:      action,
		done:        make(chan ScriptRunResult, 1),
		startedAt:   item.StartedAt,
		createdAt:   item.CreatedAt,
	}, nil
}

func safeRelativePath(root, relative string) (string, error) {
	cleanRelative := filepath.Clean(strings.TrimPrefix(strings.ReplaceAll(relative, "\\", "/"), "/"))
	if cleanRelative == "." || strings.HasPrefix(cleanRelative, "..") || filepath.IsAbs(cleanRelative) {
		return "", fmt.Errorf("路径无效")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	fullPath, err := filepath.Abs(filepath.Join(root, cleanRelative))
	if err != nil {
		return "", err
	}
	if fullPath != rootAbs && !strings.HasPrefix(fullPath, rootAbs+string(os.PathSeparator)) {
		return "", fmt.Errorf("路径越界")
	}
	return fullPath, nil
}

func validatePluginEntry(root, runtimeName, entry string) (string, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return "", fmt.Errorf("入口文件不能为空")
	}
	entry = strings.ReplaceAll(entry, "\\", "/")
	if strings.HasPrefix(entry, "/") || filepath.IsAbs(entry) {
		return "", fmt.Errorf("入口文件必须是相对路径")
	}
	cleanEntry := filepath.Clean(entry)
	if cleanEntry == "." || cleanEntry == ".." || strings.HasPrefix(cleanEntry, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("入口文件路径越界")
	}
	if err := validatePluginEntryExt(runtimeName, cleanEntry); err != nil {
		return "", err
	}
	fullPath, err := safeRelativePath(root, cleanEntry)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return "", fmt.Errorf("入口文件不存在: %w", err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("入口文件不能是目录")
	}
	return filepath.ToSlash(cleanEntry), nil
}

func validatePluginEntryExt(runtimeName, entry string) error {
	switch runtimeName {
	case "python":
		if strings.EqualFold(filepath.Ext(entry), ".py") {
			return nil
		}
		return fmt.Errorf("Python 插件入口文件必须是 .py")
	case "nodejs":
		ext := strings.ToLower(filepath.Ext(entry))
		if ext == ".js" || ext == ".mjs" || ext == ".cjs" {
			return nil
		}
		return fmt.Errorf("Node.js 插件入口文件必须是 .js、.mjs 或 .cjs")
	case "shell":
		if strings.EqualFold(filepath.Ext(entry), ".sh") {
			return nil
		}
		return fmt.Errorf("Shell 插件入口文件必须是 .sh")
	default:
		return fmt.Errorf("不支持的运行时: %s", runtimeName)
	}
}

func pluginEntryPath(root, runtimeName, entry string) (string, error) {
	cleanEntry, err := validatePluginEntry(root, runtimeName, entry)
	if err != nil {
		return "", err
	}
	return safeRelativePath(root, cleanEntry)
}

func (m *Manager) loadPluginConfig(pluginPath string) (*types.Plugin, error) {
	configPath := filepath.Join(pluginPath, "plugin.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var config types.PluginConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	pluginID := filepath.Base(pluginPath)
	config.Runtime = normalizePluginRuntime(config.Runtime)
	entry, err := validatePluginEntry(pluginPath, config.Runtime, config.Entry)
	if err != nil {
		return nil, fmt.Errorf("插件入口文件无效: %w", err)
	}
	return &types.Plugin{
		ID:                pluginID,
		Name:              config.Name,
		Version:           config.Version,
		Runtime:           config.Runtime,
		RuntimeProfile:    strings.TrimSpace(config.RuntimeProfile),
		Entry:             entry,
		Platforms:         config.Platforms,
		AllowedAdapterIDs: config.AllowedAdapterIDs,
		Priority:          config.Priority,
		Pinned:            config.Pinned,
		Trigger:           config.Trigger,
		Enabled:           config.Enabled,
		UserConfig:        config.UserConfig,
		AccessControl:     pluginAccessControl(config.AccessControl),
		OpenAPI:           normalizeOpenAPIConfig(config.OpenAPI, config.Runtime),
		ScriptEnv:         normalizeScriptEnvConfig(config.ScriptEnv),
		WebUI:             normalizePluginWebUIConfig(config.WebUI),
		WebChat:           normalizePluginWebChatConfig(config.WebChat),
		Template:          config.Template,
		TemplateVersion:   config.TemplateVersion,
		TemplateMetadata:  config.TemplateMetadata,
	}, nil
}

func normalizePluginRuntime(runtime string) string {
	runtime = strings.ToLower(strings.TrimSpace(runtime))
	if runtime == "node" {
		return "nodejs"
	}
	if runtime == "py" || runtime == "python3" {
		return "python"
	}
	return runtime
}

func normalizeOpenAPIConfig(config types.OpenAPIConfig, runtime string) types.OpenAPIConfig {
	config.Path = strings.Trim(strings.TrimSpace(config.Path), "/")
	config.Method = strings.ToUpper(strings.TrimSpace(config.Method))
	if config.Method == "" {
		config.Method = "POST"
	}
	config.Token = strings.TrimSpace(config.Token)
	config.Runtime = strings.TrimSpace(config.Runtime)
	if config.Runtime == "" {
		config.Runtime = runtime
	}
	config.RuntimeProfile = strings.TrimSpace(config.RuntimeProfile)
	return config
}

func normalizeScriptEnvConfig(config types.ScriptEnvConfig) types.ScriptEnvConfig {
	names := make([]string, 0, len(config.Names))
	seen := map[string]bool{}
	for _, name := range config.Names {
		name = strings.TrimSpace(name)
		if name == "" || strings.ContainsAny(name, "=\x00") || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	config.Names = names
	return config
}

func normalizePluginWebUIConfig(config types.PluginWebUIConfig) types.PluginWebUIConfig {
	config.Title = strings.TrimSpace(config.Title)
	config.Entry = strings.TrimSpace(strings.ReplaceAll(config.Entry, "\\", "/"))
	config.Icon = strings.TrimSpace(config.Icon)
	return config
}

func normalizePluginWebChatConfig(config types.PluginWebChatConfig) types.PluginWebChatConfig {
	config.Title = strings.TrimSpace(config.Title)
	config.Description = strings.TrimSpace(config.Description)
	config.Placeholder = strings.TrimSpace(config.Placeholder)
	config.EntryText = strings.TrimSpace(config.EntryText)
	quickActions := make([]types.PluginWebChatQuickAction, 0, len(config.QuickActions))
	for _, action := range config.QuickActions {
		action.Label = strings.TrimSpace(action.Label)
		action.Text = strings.TrimSpace(action.Text)
		if action.Label == "" {
			action.Label = action.Text
		}
		if action.Label == "" || action.Text == "" {
			continue
		}
		quickActions = append(quickActions, action)
	}
	config.QuickActions = quickActions
	keywords := make([]string, 0, len(config.Keywords))
	seen := map[string]bool{}
	for _, keyword := range config.Keywords {
		keyword = strings.TrimSpace(keyword)
		if keyword == "" || seen[keyword] {
			continue
		}
		seen[keyword] = true
		keywords = append(keywords, keyword)
	}
	config.Keywords = keywords
	return config
}

func pluginAccessControl(config *types.AccessControlConfig) types.AccessControlConfig {
	if config == nil {
		return types.AccessControlConfig{InheritSystem: true}
	}
	return *config
}

func (m *Manager) installDeps(plugin *types.Plugin) {
	configPath := filepath.Join(m.pluginDir, plugin.ID, "plugin.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return
	}
	var config types.PluginConfig
	if err := json.Unmarshal(data, &config); err != nil || len(config.Dependencies) == 0 {
		return
	}
	switch normalizePluginRuntime(config.Runtime) {
	case "python":
		if err := m.depsManager.EnsurePythonDepsForProfile(config.RuntimeProfile, config.Dependencies); err != nil {
			log.Printf("[SYSTEM] 确保插件 %s Python 依赖失败: %v", config.Name, err)
		}
	case "nodejs":
		if err := m.depsManager.EnsureNodeDepsForProfile(config.RuntimeProfile, config.Dependencies); err != nil {
			log.Printf("[SYSTEM] 确保插件 %s Node.js 依赖失败: %v", config.Name, err)
		}
	}
}
