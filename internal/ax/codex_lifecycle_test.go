package ax

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const observedThread = "11111111-1111-4111-8111-111111111111"

type codexLifecycleFixture struct {
	t       *testing.T
	admin   *client
	bridge  *client
	session Session
	remote  string
	backend chan *websocket.Conn
	front   *websocket.Conn
}

func newCodexLifecycleFixture(t *testing.T, reports ...chan error) *codexLifecycleFixture {
	t.Helper()
	dir := startTestServer(t)
	s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "api", Host: "codex", Mesh: testMesh, LaunchID: "test-launch"}
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	admin, err := dial(socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.close)
	if err := admin.call("ax.enroll", s, nil); err != nil {
		t.Fatal(err)
	}
	bridge, err := dial(socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bridge.close)
	caps, _ := sessionCapabilities(s)
	if err := bridge.call("ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret, "launch_id": s.LaunchID, "delivery_capabilities": caps}, nil); err != nil {
		t.Fatal(err)
	}
	if err := bridge.call("ax.ready", object{}, nil); err != nil {
		t.Fatal(err)
	}
	backend := make(chan *websocket.Conn, 4)
	listener, err := net.Listen("unix", filepath.Join(dir, "native.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			backend <- conn
		}
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	var startupErrors chan<- error
	if len(reports) != 0 {
		startupErrors = reports[0]
	}
	remote, cleanup, err := codexSessionSocket(context.Background(), dir, file, "unix://"+listener.Addr().String(), false, startupErrors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	return &codexLifecycleFixture{t: t, admin: admin, bridge: bridge, session: s, remote: remote, backend: backend}
}

func (f *codexLifecycleFixture) connect(status string) *websocket.Conn {
	f.t.Helper()
	front, err := dialCodex(context.Background(), f.remote)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { front.Close() })
	f.front = front
	back := <-f.backend
	f.t.Cleanup(func() { back.Close() })
	if err := front.WriteJSON(object{"id": 1, "method": "thread/resume", "params": object{"threadId": observedThread}}); err != nil {
		f.t.Fatal(err)
	}
	var request packet
	back.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := back.ReadJSON(&request); err != nil || request.Method != "thread/resume" {
		f.t.Fatalf("native selection: %+v %v", request, err)
	}
	thread := object{"id": observedThread}
	if status != "" {
		thread["status"] = object{"type": status}
	}
	f.send(back, object{"id": 1, "result": object{"thread": thread, "sandbox": object{"type": "readOnly"}}})
	return back
}

func (f *codexLifecycleFixture) send(back *websocket.Conn, event object) {
	f.t.Helper()
	if err := back.WriteJSON(event); err != nil {
		f.t.Fatal(err)
	}
	f.front.SetReadDeadline(time.Now().Add(3 * time.Second))
	var got object
	if err := f.front.ReadJSON(&got); err != nil || string(raw(got)) != string(raw(event)) {
		f.t.Fatalf("native frame changed: %s %v", raw(got), err)
	}
}

func (f *codexLifecycleFixture) status(back *websocket.Conn, thread, state string) {
	f.send(back, object{"method": "thread/status/changed", "params": object{"threadId": thread, "status": object{"type": state}}})
}

func (f *codexLifecycleFixture) waitState(want string) discoveredAgent {
	f.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var agents []discoveredAgent
		if err := f.admin.call("ax.inspect", object{}, &agents); err != nil {
			f.t.Fatal(err)
		}
		for _, a := range agents {
			if a.ID == f.session.ID && a.State == want {
				return a
			}
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("want %s, got %+v", want, agents)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCodexObservedLifecycle(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	back := f.connect("idle")
	a := f.waitState("ready")
	if a.Capabilities == nil || !reflect.DeepEqual(a.Capabilities.Lifecycle, []string{"session_bound", "busy", "idle"}) {
		t.Fatalf("native lifecycle not advertised: %+v", a.Capabilities)
	}
	f.status(back, "22222222-2222-4222-8222-222222222222", "active")
	f.waitState("ready")
	f.status(back, observedThread, "active")
	f.waitState("busy")
	if err := f.bridge.call("ax.presence", object{"native_session_id": observedThread, "permission_mode": "read-only", "state": "ready"}, nil); err != nil {
		t.Fatal(err)
	}
	f.waitState("busy")
	f.status(back, observedThread, "idle")
	f.waitState("ready")
	back.Close()
	a = f.waitState("blocked")
	if a.Reason != "Recipient's native Codex connection is unavailable; mail remains queued." {
		t.Fatalf("missing disconnect reason: %q", a.Reason)
	}
	back = f.connect("active")
	f.waitState("busy")
	f.status(back, observedThread, "idle")
	f.waitState("ready")
}

func TestCodexLegacySelectionKeepsUnprovenLifecycle(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	f.connect("")
	a := f.waitState("ready")
	if a.Capabilities == nil || !reflect.DeepEqual(a.Capabilities.Lifecycle, []string{"session_bound"}) {
		t.Fatalf("legacy backend claimed lifecycle observation: %+v", a.Capabilities)
	}
}

func TestCodexBusyKeepsNativeQueueFIFO(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	f.connect("active")
	f.waitState("busy")
	from := f.sender()
	for _, text := range []string{"first", "second"} {
		if err := from.call("ax.send", object{"target": "api", "text": text, "client_message_id": text}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"first", "second"} {
		select {
		case offer := <-f.bridge.offers:
			if offer.Text != want {
				t.Fatalf("busy FIFO: want %s, got %s", want, offer.Text)
			}
			if err := f.bridge.call("ax.receipt", object{"message_id": offer.ID, "handoff_id": offer.HandoffID, "receipt": "wake_accepted"}, nil); err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("busy Codex held native-queue mail")
		}
	}
	f.waitState("busy")
}

func (f *codexLifecycleFixture) sender() *client {
	t := f.t
	t.Helper()
	sender := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "web", Host: "codex", Mesh: testMesh, Native: "22222222-2222-4222-8222-222222222222"}
	if err := f.admin.call("ax.enroll", sender, nil); err != nil {
		t.Fatal(err)
	}
	from, err := dial(f.admin.conn.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(from.close)
	for _, call := range []struct {
		method string
		args   object
	}{
		{"ax.connect", object{"version": "1", "agent_id": sender.ID, "secret": sender.Secret, "interrupted_notices": true}},
		{"ax.presence", object{"native_session_id": sender.Native, "permission_mode": "read-only", "state": "ready"}},
		{"ax.ready", object{}},
	} {
		if err := from.call(call.method, call.args, nil); err != nil {
			t.Fatal(err)
		}
	}
	return from
}

func TestCodexObserverFiltersBeforeBinding(t *testing.T) {
	o := codexSessionObserver{pending: map[string]bool{}}
	for _, method := range []string{"thread/status/changed", "turn/started", "turn/completed"} {
		if got := o.response(raw(object{"method": method, "params": object{"threadId": observedThread, "status": object{"type": "active"}, "turn": object{"id": "turn-1", "status": "interrupted"}}})); got != nil {
			t.Fatalf("unselected event bound AX: %s", got)
		}
	}
	// Keep decoding expectations independent from the implementation's structs.
	o.request(raw(object{"id": 1, "method": "thread/resume"}))
	var event map[string]any
	json.Unmarshal(o.response(raw(object{"id": 1, "result": object{"thread": object{"id": observedThread, "status": object{"type": "active"}}}})), &event)
	if event["codex_status"] != "active" {
		t.Fatalf("lost selected runtime state: %+v", event)
	}
}

func TestCodexSystemErrorRemainsAvailable(t *testing.T) {
	if got := codexLifecycleState("systemError"); got != "ready" {
		t.Fatalf("systemError accepts native turns: want ready, got %q", got)
	}
	f := newCodexLifecycleFixture(t)
	back := f.connect("systemError")
	a := f.waitState("ready")
	if a.Reason != "" {
		t.Fatalf("available backend reported unavailable: %q", a.Reason)
	}
	f.status(back, observedThread, "active")
	f.waitState("busy")
}

func TestCodexOptionalStatusShapeDoesNotPreventBinding(t *testing.T) {
	for _, status := range []any{nil, "idle", 7, []string{"idle"}} {
		o := codexSessionObserver{pending: map[string]bool{}}
		o.request(raw(object{"id": 1, "method": "thread/start"}))
		var event object
		got := o.response(raw(object{"id": 1, "result": object{"thread": object{"id": observedThread, "status": status}}}))
		if json.Unmarshal(got, &event) != nil || event["session_id"] != "11111111-1111-4111-8111-111111111111" || event["codex_status"] != "" {
			t.Fatalf("optional status prevented binding: %s", got)
		}
	}
}

func TestCodexRepeatedSnapshotDoesNotRewriteSession(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	f.connect("idle")
	f.waitState("ready")
	dir := filepath.Dir(f.admin.conn.RemoteAddr().String())
	file := filepath.Join(dir, "session.json")
	stamp := time.Unix(1234567000, 0)
	if err := os.Chtimes(file, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		event := raw(object{"hook_event_name": "CodexState", "session_id": observedThread, "permission_mode": "read-only", "codex_status": "idle"})
		if err := Hook(dir, file, bytes.NewReader(event), f.session.LaunchID); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(file)
	if err != nil || info.ModTime().Unix() != 1234567000 {
		t.Fatalf("unchanged snapshot rewrote the session: %v %v", info, err)
	}
}

func TestCodexOlderSelectingConnectionCanResumeObservation(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	first := f.connect("idle")
	firstFront := f.front
	f.waitState("ready")
	second := f.connect("active")
	f.waitState("busy")
	second.Close()
	f.front.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := f.front.ReadMessage(); err == nil {
		t.Fatal("second native connection stayed open")
	}
	f.front = firstFront
	f.status(first, observedThread, "active")
	f.waitState("busy")
	f.status(first, observedThread, "idle")
	f.waitState("ready")
}

func TestCodexCleanupBoundsHungBroker(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ax-hung-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "api", Host: "codex", Mesh: testMesh}
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requestSeen, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := readFrame(conn); err == nil {
			close(requestSeen)
		}
		<-release
	}()
	backend, err := net.Listen("unix", filepath.Join(dir, "native.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var p packet
		if conn.ReadJSON(&p) == nil {
			conn.WriteJSON(object{"id": p.ID, "result": object{"thread": object{"id": observedThread, "status": object{"type": "idle"}}, "sandbox": object{"type": "readOnly"}}})
		}
		<-release
	})}
	go server.Serve(backend)
	defer server.Close()
	remote, cleanup, err := codexSessionSocket(context.Background(), dir, file, "unix://"+backend.Addr().String(), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	front, err := dialCodex(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	defer front.Close()
	front.WriteJSON(object{"id": 1, "method": "thread/start"})
	var reply packet
	front.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := front.ReadJSON(&reply); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("lifecycle request did not reach the hung broker")
	}
	done := make(chan struct{})
	go func() { cleanup(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("native cleanup waited for the hung broker")
	}
}

func TestCodexBindingConflictReportsOnce(t *testing.T) {
	reports := make(chan error, 8)
	f := newCodexLifecycleFixture(t, reports)
	back := f.connect("idle")
	f.waitState("ready")
	other := "22222222-2222-4222-8222-222222222222"
	if err := f.front.WriteJSON(object{"id": 2, "method": "thread/resume", "params": object{"threadId": other}}); err != nil {
		t.Fatal(err)
	}
	back.SetReadDeadline(time.Now().Add(3 * time.Second))
	var selection packet
	if err := back.ReadJSON(&selection); err != nil || string(selection.ID) != "2" {
		t.Fatalf("native selection not forwarded: %+v %v", selection, err)
	}
	f.send(back, object{"id": 2, "result": object{"thread": object{"id": other, "status": object{"type": "idle"}}, "sandbox": object{"type": "readOnly"}}})
	count := 0
	select {
	case err := <-reports:
		count++
		if err == nil || !strings.Contains(err.Error(), "belongs to conversation") {
			t.Fatalf("wrong startup error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("binding conflict was not reported")
	}
	f.waitState("binding-conflict")
	f.status(back, other, "active")
	timer := time.NewTimer(6 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-reports:
			count++
		case <-timer.C:
			if count != 1 {
				t.Fatalf("binding conflict must be reported once, got %d reports", count)
			}
			return
		}
	}
}
