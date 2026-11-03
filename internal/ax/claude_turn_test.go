package ax

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestClaudeTurnCompleteClearsPermissionBlock(t *testing.T) {
	dir := startTestServer(t)
	s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "api", Host: "claude", Mesh: testMesh, Native: observedThread, Started: true}
	c := connectDiscoveryPeer(t, dir, s)
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	for _, event := range []object{
		{"hook_event_name": "ClaudeTurnStart", "session_id": observedThread, "claude_turn": object{"id": "permission-turn", "status": "inProgress", "at_ms": 1000}},
		{"hook_event_name": "PermissionRequest", "session_id": observedThread, "tool_name": "Bash"},
		{"hook_event_name": "ClaudeTurnComplete", "session_id": observedThread, "claude_turn": object{"id": "permission-turn", "status": "completed", "at_ms": 6000}},
	} {
		if err := Hook(dir, file, bytes.NewReader(raw(event))); err != nil {
			t.Fatal(err)
		}
	}
	if got := discoveryList(t, c)[s.ID]; got.State != "ready" {
		t.Fatalf("native completion left permission hold: %+v", got)
	}
}

func TestClaudeDelayedCompletionCannotOverrideNextTurn(t *testing.T) {
	b, _ := localBroker(t)
	r, _ := claudeInterruptRecipient(t, b)
	for _, event := range []struct {
		id, status, state string
		at                int64
	}{{"first", "inProgress", "busy", 1000}, {"first", "interrupted", "ready", 2000}, {"next", "inProgress", "busy", 3000}, {"first", "interrupted", "ready", 2000}} {
		request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": r.ID, "secret": r.Secret, "native_session_id": r.Native, "state": event.state, "claude_turn": object{"id": event.id, "status": event.status, "at_ms": event.at}})
	}
	if p := b.peers[r.ID]; p.State != "busy" || p.claudeTurn == nil || p.claudeTurn.ID != "next" {
		t.Fatalf("late completion replaced active turn: %+v", p)
	}
}

func TestClaudeDelayedStartCannotReopenCompletedTurn(t *testing.T) {
	for _, status := range []string{"completed", "failed", "interrupted"} {
		for _, started := range []bool{false, true} {
			for _, state := range []string{"ready", "busy", "blocked"} {
				t.Run(fmt.Sprintf("%s/started=%t/%s", status, started, state), func(t *testing.T) {
					b, _ := localBroker(t)
					s, _ := claudeInterruptRecipient(t, b)
					if started {
						nativeClaudeTurn(t, b, s, "finished", "inProgress", 1000)
					}
					nativeClaudeTurn(t, b, s, "finished", status, 2000)
					if state != "ready" {
						nativeClaudeTurn(t, b, s, "current", "inProgress", 3000)
						request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "state": state})
					}
					nativeClaudeTurn(t, b, s, "finished", "inProgress", 1000)
					p := b.peers[s.ID]
					if p.State != state {
						t.Fatalf("late start changed %s to %s", state, p.State)
					}
					if state != "ready" && (p.claudeTurn == nil || p.claudeTurn.ID != "current") {
						t.Fatalf("late start replaced current turn: %+v", p.claudeTurn)
					}
				})
			}
		}
	}
}

func TestClaudeTerminalHistoryPreservesActiveTurn(t *testing.T) {
	b, _ := localBroker(t)
	s, _ := claudeInterruptRecipient(t, b)
	nativeClaudeTurn(t, b, s, "current", "inProgress", 1000)
	for i := 0; i < 32; i++ {
		nativeClaudeTurn(t, b, s, fmt.Sprintf("finished-%d", i), "completed", int64(2000+i))
	}
	nativeClaudeTurn(t, b, s, "finished-31", "inProgress", 1500)
	if p := b.peers[s.ID]; p.State != "busy" || p.claudeTurn.ID != "current" {
		t.Fatalf("terminal history lost current activity: %+v", p.claudeTurn)
	}
	nativeClaudeTurn(t, b, s, "current", "interrupted", 4000)
	if p := b.peers[s.ID]; p.State != "ready" || len(p.claudeTurns) > 16 {
		t.Fatalf("active turn lost or history unbounded: state=%s turns=%d", p.State, len(p.claudeTurns))
	}
}

func TestClaudeCompletionWithoutStartPreservesCurrentActivity(t *testing.T) {
	for _, tc := range []struct {
		name, state, want string
		previous, active  bool
	}{
		{name: "first turn canceled before start", state: "busy", want: "ready"},
		{name: "later turn canceled before start", state: "busy", want: "ready", previous: true},
		{name: "newer turn busy", state: "busy", want: "busy", active: true},
		{name: "newer turn blocked", state: "blocked", want: "blocked", active: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := localBroker(t)
			s, _ := claudeInterruptRecipient(t, b)
			if tc.previous {
				nativeClaudeTurn(t, b, s, "previous", "inProgress", 1000)
				nativeClaudeTurn(t, b, s, "previous", "completed", 2000)
			}
			if tc.active {
				nativeClaudeTurn(t, b, s, "newer", "inProgress", 3000)
			}
			request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "state": tc.state})
			nativeClaudeTurn(t, b, s, "unobserved", "interrupted", 4000)
			if got := b.peers[s.ID].State; got != tc.want {
				t.Fatalf("unmatched completion changed %s to %s; want %s", tc.state, got, tc.want)
			}
		})
	}
}

func TestClaudeInterruptedHookLeavesBusy(t *testing.T) {
	dir := startTestServer(t)
	s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "api", Host: "claude", Mesh: testMesh, Native: observedThread, Started: true}
	c := connectDiscoveryPeer(t, dir, s)
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	for _, event := range []object{
		{"hook_event_name": "ClaudeTurnStart", "session_id": observedThread, "claude_turn": object{"id": "turn-1", "status": "inProgress", "at_ms": 1000}},
		{"hook_event_name": "UserPromptSubmit", "session_id": observedThread},
		{"hook_event_name": "ContextUsage", "session_id": observedThread, "context_usage": object{"context_used_tokens": 32000, "context_window_tokens": 128000, "context_observed_at_ms": 1000}},
	} {
		if err := Hook(dir, file, bytes.NewReader(raw(event))); err != nil {
			t.Fatal(err)
		}
		if a := discoveryList(t, c)[s.ID]; a.State != "busy" {
			t.Fatalf("context changed busy state: %+v", a)
		}
	}
	if err := Hook(dir, file, bytes.NewReader(raw(object{"hook_event_name": "ClaudeTurnComplete", "session_id": observedThread, "claude_turn": object{"id": "turn-1", "status": "interrupted", "at_ms": time.Now().UnixMilli()}}))); err != nil {
		t.Fatal(err)
	}
	if a := discoveryList(t, c)[s.ID]; a.State != "ready" {
		t.Fatalf("aborted Claude stayed busy: %+v", a)
	}
}

func claudeInterruptRecipient(t *testing.T, b *broker) (Session, *serverConn) {
	t.Helper()
	s, c, _ := endpoint(t, b, "api", testMesh)
	s.Host = "claude"
	p := b.peers[s.ID]
	p.Host, p.Permission = "claude", "default"
	p.capabilities, _ = sessionCapabilities(s)
	if err := b.save(p); err != nil {
		t.Fatal(err)
	}
	return s, c
}

func nativeClaudeTurn(t *testing.T, b *broker, s Session, id, status string, at int64) {
	t.Helper()
	state := "ready"
	if status == "inProgress" {
		state = "busy"
	}
	request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "state": state, "claude_turn": object{"id": id, "status": status, "at_ms": at}})
}

func TestClaudeDelayedOlderStartCannotReplaceNewerTurn(t *testing.T) {
	b, _ := localBroker(t)
	s, _ := claudeInterruptRecipient(t, b)
	nativeClaudeTurn(t, b, s, "newer", "inProgress", 2000)
	nativeClaudeTurn(t, b, s, "older", "inProgress", 1000)
	nativeClaudeTurn(t, b, s, "older", "interrupted", 1500)
	if p := b.peers[s.ID]; p.State != "busy" || p.claudeTurn == nil || p.claudeTurn.ID != "newer" {
		t.Fatalf("an older turn's delayed reports replaced the running turn: state=%s turn=%+v", p.State, p.claudeTurn)
	}
	nativeClaudeTurn(t, b, s, "newer", "completed", 3000)
	if got := b.peers[s.ID].State; got != "ready" {
		t.Fatalf("running turn's completion left %s", got)
	}
}
