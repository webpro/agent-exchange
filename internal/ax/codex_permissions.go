package ax

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// The remote TUI rejects permission flags on resume. Apply an explicit bypass
// choice to the native session request instead, before its first turn can run.
func codexBypassArgs(args []string) ([]string, bool, error) {
	var out []string
	bypass, conflict := false, false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			out = append(out, args[i:]...)
			break
		}
		if arg == "--dangerously-bypass-approvals-and-sandbox" || arg == "--yolo" {
			bypass = true
			continue
		}
		key, _, _ := strings.Cut(arg, "=")
		if key == "--sandbox" || key == "--ask-for-approval" || key == "--approve-for-me" || key == "--not-so-yolo" || (strings.HasPrefix(key, "-s") && !strings.HasPrefix(key, "--")) || (strings.HasPrefix(key, "-a") && !strings.HasPrefix(key, "--")) {
			conflict = true
		}
		out = append(out, arg)
		// Do not interpret option values as AX's permission choice.
		switch arg {
		case "-c", "--config", "-m", "--model", "-p", "--profile", "-s", "--sandbox", "-a", "--ask-for-approval", "-i", "--image", "-C", "--cd", "--add-dir", "--enable", "--disable", "--local-provider", "--remote", "--remote-auth-token-env":
			if i+1 < len(args) {
				i++
				out = append(out, args[i])
			}
		}
	}
	if bypass && conflict {
		return nil, false, fmt.Errorf("choose bypass or separate sandbox/approval options, not both")
	}
	return out, bypass, nil
}

// Only the TUI uses this private adapter. AX's queue and metadata connections go
// directly to the backend. No peer message can select or change permissions.
func codexSessionSocket(ctx context.Context, dir, file, remote string, bypass bool, startupErrors chan<- error) (string, func(), error) {
	session, err := loadSession(file)
	if err != nil {
		return "", nil, err
	}
	path := filepath.Join(dir, randomID("permissions_")+".sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		return "", nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		listener.Close()
		return "", nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	// One bounded worker keeps broker failures off every native transport path.
	type startupPermission struct {
		mode      string
		delivered bool
	}
	type observation struct {
		connection  uint64
		data        []byte
		native      string
		interrupted *nativeTurn
		permission  *startupPermission
	}
	events := make(chan struct{}, 1)
	var observed sync.Mutex
	var latest observation
	live := make(map[uint64]observation)
	var connections atomic.Uint64
	lifecycleDone := make(chan struct{})
	go func() {
		defer close(lifecycleDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-events:
			case <-ticker.C:
			}
			observed.Lock()
			data := latest.data
			permission := latest.permission
			if data != nil && permission != nil && !permission.delivered {
				var fields object
				json.Unmarshal(data, &fields)
				fields["permission_mode"] = permission.mode
				fields["hook_event_name"] = "SessionStart"
				data = raw(fields)
			}
			observed.Unlock()
			if data == nil {
				continue
			}
			if err := Hook(dir, file, bytes.NewReader(data), session.LaunchID); err != nil {
				if saved, e := loadSession(file); e == nil && saved.BindingError != "" {
					select {
					case startupErrors <- err:
					default:
					}
					return
				}
				continue
			}
			if permission != nil {
				observed.Lock()
				permission.delivered = true
				observed.Unlock()
			}
		}
	}()
	server := &http.Server{ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection := connections.Add(1)
		emit := func(data []byte, closed bool) {
			if data == nil && !closed {
				return
			}
			observed.Lock()
			previous := latest
			var fields object
			json.Unmarshal(data, &fields)
			permission, _ := fields["permission_mode"].(string)
			if permission != "" {
				delete(fields, "permission_mode")
				data = raw(fields)
			}
			if closed {
				delete(live, connection)
			} else {
				live[connection] = observation{connection: connection, data: data}
			}
			if len(live) == 0 {
				if data != nil && connection >= latest.connection {
					latest = observation{connection: connection, data: data}
				}
			} else {
				latest = observation{}
				for id, candidate := range live {
					if id > latest.connection {
						latest = candidate
					}
				}
			}
			var event struct {
				Native      string      `json:"session_id"`
				Interrupted *nativeTurn `json:"codex_interrupted"`
			}
			json.Unmarshal(latest.data, &event)
			if event.Native == previous.native {
				latest.permission = previous.permission
			}
			if permission != "" && connection == latest.connection {
				latest.permission = &startupPermission{mode: permission}
			}
			if event.Native == previous.native && previous.interrupted != nil && (event.Interrupted == nil || event.Interrupted.At < previous.interrupted.At) {
				var fields object
				json.Unmarshal(latest.data, &fields)
				event.Interrupted = previous.interrupted
				fields["codex_interrupted"] = event.Interrupted
				latest.data = raw(fields)
			}
			latest.native, latest.interrupted = event.Native, event.Interrupted
			observed.Unlock()
			select {
			case events <- struct{}{}:
			default:
			}
		}
		backend, err := dialCodex(ctx, remote)
		if err != nil {
			http.Error(w, "Codex backend unavailable", http.StatusBadGateway)
			return
		}
		defer backend.Close()
		front, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer front.Close()
		front.SetReadLimit(64 << 20)
		backend.SetReadLimit(64 << 20)
		stop := context.AfterFunc(ctx, func() { front.Close(); backend.Close() })
		defer stop()
		observer := &codexSessionObserver{pending: make(map[string]bool)}
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer front.Close()
			defer func() { emit(observer.unavailable(), true) }()
			for {
				kind, data, err := backend.ReadMessage()
				if err != nil || front.WriteMessage(kind, data) != nil {
					return
				}
				emit(observer.response(data), false)
			}
		}()
		for {
			kind, data, err := front.ReadMessage()
			if err != nil {
				break
			}
			observer.request(data)
			if bypass {
				data, err = codexBypassRequest(data)
			}
			if err != nil || backend.WriteMessage(kind, data) != nil {
				break
			}
		}
		backend.Close()
		<-done
	})}
	go server.Serve(limitListener(listener, 8))
	return "unix://" + path, func() {
		cancel()
		server.Close()
		select {
		case <-lifecycleDone:
		case <-time.After(time.Second):
		}
		os.Remove(path)
	}, nil
}

// Only the TUI's own conversation selection can bind the observation. Native
// events for other threads, including session naming and subagents, are ignored.
type codexSessionObserver struct {
	mu          sync.Mutex
	pending     map[string]bool
	native      string
	status      string
	turn        *nativeTurn
	interrupted *nativeTurn
	usage       *ContextUsage
}

func (o *codexSessionObserver) request(data []byte) {
	var p packet
	if json.Unmarshal(data, &p) != nil || len(p.ID) == 0 || len(p.ID) > 128 {
		return
	}
	switch p.Method {
	case "thread/start", "thread/resume", "thread/fork":
		o.mu.Lock()
		defer o.mu.Unlock()
		if len(o.pending) < 16 {
			o.pending[string(p.ID)] = true
		}
	}
}

func (o *codexSessionObserver) response(data []byte) []byte {
	var p packet
	if json.Unmarshal(data, &p) != nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.Method != "" {
		if p.Method != "thread/status/changed" && p.Method != "turn/started" && p.Method != "turn/completed" && p.Method != "thread/tokenUsage/updated" && p.Method != "item/started" {
			return nil
		}
		var params struct {
			Thread string                `json:"threadId"`
			Status struct{ Type string } `json:"status"`
			Turn   nativeTurn            `json:"turn"`
			Usage  *struct {
				Last struct {
					Total *int64 `json:"totalTokens"`
				} `json:"last"`
				Window *int64 `json:"modelContextWindow"`
			} `json:"tokenUsage"`
			Item struct{ Type string } `json:"item"`
		}
		if json.Unmarshal(p.Params, &params) != nil || o.native == "" || params.Thread != o.native {
			return nil
		}
		if p.Method == "thread/tokenUsage/updated" {
			if params.Usage == nil {
				return nil
			}
			u := &ContextUsage{UsedTokens: params.Usage.Last.Total, WindowTokens: params.Usage.Window, ObservedAt: time.Now().UnixMilli()}
			if !u.valid() {
				return nil
			}
			o.usage = u
		} else if p.Method == "item/started" {
			if params.Item.Type != "contextCompaction" {
				return nil
			}
			u := &ContextUsage{ObservedAt: time.Now().UnixMilli()}
			if o.usage != nil {
				u.WindowTokens = o.usage.WindowTokens
			}
			o.usage = u
		} else if p.Method == "thread/status/changed" {
			if params.Status.Type == o.status || codexLifecycleState(params.Status.Type) == "" {
				return nil
			}
			o.status = params.Status.Type
			if o.status == "active" && o.turn != nil && o.turn.Status != "inProgress" {
				o.turn = nil
			}
		} else {
			params.Turn.At = time.Now().UnixMilli()
			if !validNativeTurn(&params.Turn) || (o.turn != nil && o.turn.ID == params.Turn.ID && o.turn.Status == params.Turn.Status) {
				return nil
			}
			if (p.Method == "turn/started") != (params.Turn.Status == "inProgress") {
				return nil
			}
			if p.Method == "turn/completed" && o.turn != nil && o.turn.ID != params.Turn.ID {
				return nil
			}
			o.turn = &params.Turn
			if p.Method == "turn/started" {
				o.status = "active"
			} else {
				o.status = "idle"
				if params.Turn.Status == "interrupted" {
					o.interrupted = o.turn
				}
			}
		}
		return o.event()
	}
	matched := o.pending[string(p.ID)]
	delete(o.pending, string(p.ID))
	if !matched || p.Error != nil {
		return nil
	}
	var result struct {
		Thread struct {
			ID     string
			Status json.RawMessage
		} `json:"thread"`
		Sandbox struct{ Type string } `json:"sandbox"`
	}
	if json.Unmarshal(p.Result, &result) != nil || !validNative(result.Thread.ID) {
		return nil
	}
	var status struct{ Type string }
	_ = json.Unmarshal(result.Thread.Status, &status)
	if o.native != result.Thread.ID {
		o.interrupted = nil
	}
	o.native, o.status = result.Thread.ID, status.Type
	o.turn = nil
	o.usage = &ContextUsage{ObservedAt: time.Now().UnixMilli()}
	permission := "unknown"
	switch result.Sandbox.Type {
	case "readOnly":
		permission = "read-only"
	case "workspaceWrite":
		permission = "workspace-write"
	case "dangerFullAccess":
		permission = "danger-full-access"
	}
	event := object{"hook_event_name": "SessionStart", "session_id": result.Thread.ID, "permission_mode": permission, "codex_status": o.status, "context_usage": o.usage}
	return raw(event)
}

func (o *codexSessionObserver) event() []byte {
	event := "CodexState"
	if codexLifecycleState(o.status) == "" {
		event = "ContextUsage"
	}
	return raw(object{"hook_event_name": event, "session_id": o.native, "codex_status": o.status, "codex_interrupted": o.interrupted, "context_usage": o.usage})
}

func (o *codexSessionObserver) unavailable() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.native == "" || codexLifecycleState(o.status) == "" {
		return nil
	}
	o.status = "unavailable"
	return o.event()
}

func codexLifecycleState(status string) string {
	switch status {
	case "active":
		return "busy"
	case "idle", "systemError":
		return "ready"
	case "notLoaded", "unavailable":
		return "blocked"
	}
	return ""
}

func codexBypassRequest(data []byte) ([]byte, error) {
	var request struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, err
	}
	switch request.Method {
	case "thread/start", "thread/resume", "thread/fork":
		// Preserve unknown protocol fields and all responses/notifications verbatim.
		var envelope map[string]json.RawMessage
		var params map[string]json.RawMessage
		if err := json.Unmarshal(data, &envelope); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(envelope["params"], &params); err != nil || params == nil {
			return nil, fmt.Errorf("invalid Codex session parameters")
		}
		params["approvalPolicy"] = raw("never")
		params["sandbox"] = raw("danger-full-access")
		delete(params, "permissions") // Named and legacy sandbox choices are exclusive.
		envelope["params"] = raw(params)
		return raw(envelope), nil
	default:
		return data, nil
	}
}
