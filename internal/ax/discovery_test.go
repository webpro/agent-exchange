package ax

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func connectDiscoveryPeer(t testing.TB, dir string, s Session) *client {
	t.Helper()
	c, err := dial(socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	for _, step := range []struct {
		method string
		args   any
	}{
		{"ax.enroll", s},
		{"ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret}},
		{"ax.presence", object{"native_session_id": s.Native, "permission_mode": "default", "state": "ready"}},
		{"ax.ready", object{}},
	} {
		if err := c.call(step.method, step.args, nil); err != nil {
			t.Fatalf("%s: %v", step.method, err)
		}
	}
	return c
}

func discoveryList(t *testing.T, c *client) map[string]Agent {
	t.Helper()
	var agents []Agent
	if err := c.call("ax.list", object{}, &agents); err != nil {
		t.Fatal(err)
	}
	result := make(map[string]Agent, len(agents))
	for _, a := range agents {
		result[a.ID] = a
	}
	return result
}

func TestFourPeerDiscoveryAcrossHarnessesAndReconnects(t *testing.T) {
	dir := startTestServer(t)
	var sessions []Session
	var clients []*client
	assertViews := func(offline string) {
		t.Helper()
		for i, c := range clients {
			if sessions[i].ID == offline {
				continue
			}
			view := discoveryList(t, c)
			if len(view) != len(sessions) {
				t.Fatalf("%s sees %d peers, want %d", sessions[i].Name, len(view), len(sessions))
			}
			for _, s := range sessions {
				a, ok := view[s.ID]
				state := "ready"
				if s.ID == offline {
					state = "offline"
				}
				if !ok || a.Host != s.Host || a.Online != (s.ID != offline) || a.State != state {
					t.Fatalf("%s sees inconsistent peer %s: %+v", sessions[i].Name, s.Name, a)
				}
			}
		}
	}
	for i, host := range []string{"claude", "codex", "claude", "codex"} {
		s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: fmt.Sprintf("%s%d", host, i), Host: host, Mesh: digest(fmt.Sprint(i))[:24], Workspace: fmt.Sprintf("/repo%d", i), Native: uuid()}
		sessions = append(sessions, s)
		clients = append(clients, connectDiscoveryPeer(t, dir, s))
		assertViews("") // Existing and newly joined peers must all see each late join.
	}
	clients[0].close()
	deadline := time.Now().Add(time.Second)
	for discoveryList(t, clients[1])[sessions[0].ID].Online {
		if time.Now().After(deadline) {
			t.Fatal("disconnected Claude stayed online")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertViews(sessions[0].ID)
	clients[0] = connectDiscoveryPeer(t, dir, sessions[0])
	assertViews("")
	var out bytes.Buffer
	doctorBroker(context.Background(), dir, &out)
	if !strings.Contains(out.String(), "4 registered agents") || strings.Count(out.String(), "online=true") != 4 {
		t.Fatalf("doctor differs from authenticated discovery: %s", &out)
	}
	// Another private runtime deliberately has its own broker and agent view.
	other := startTestServer(t)
	isolate := connectDiscoveryPeer(t, other, sessions[0])
	if view := discoveryList(t, isolate); len(view) != 1 {
		t.Fatalf("different AX homes leaked agents: %+v", view)
	}
	assertViews("")
}

func TestListAgentsNamesTheRuntimeItSearched(t *testing.T) {
	dir := startTestServer(t)
	admin, err := dial(socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.close()
	s := Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: "web", Host: "claude", Mesh: testMesh, Native: uuid(), Started: true}
	if err := admin.call("ax.enroll", s, nil); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	var in, out bytes.Buffer
	json.NewEncoder(&in).Encode(packet{JSONRPC: "2.0", ID: raw(1), Method: "tools/call", Params: raw(object{"name": "list_agents", "arguments": object{}})})
	before := time.Now().UnixMilli()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := Bridge(ctx, dir, file, &in, &out); err != nil {
		t.Fatal(err)
	}
	var frame packet
	var response struct {
		IsError bool                    `json:"isError"`
		Content []struct{ Text string } `json:"content"`
	}
	if err := json.NewDecoder(&out).Decode(&frame); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(frame.Result, &response); err != nil || response.IsError || len(response.Content) != 1 {
		t.Fatalf("bad MCP response: %s %v", frame.Result, err)
	}
	var result struct {
		Agents []Agent `json:"agents"`
		Home   string  `json:"ax_home"`
		Scope  string  `json:"scope"`
		At     int64   `json:"snapshot_at_ms"`
	}
	if err := json.Unmarshal([]byte(response.Content[0].Text), &result); err != nil {
		t.Fatalf("list_agents result is not an object: %s", response.Content[0].Text)
	}
	if len(result.Agents) != 1 || result.Agents[0].Name != "web" {
		t.Fatalf("agents missing from the result: %+v", result.Agents)
	}
	if result.Home != dir {
		t.Fatalf("result named runtime %q, want %q", result.Home, dir)
	}
	if !strings.Contains(result.Scope, "AX_HOME") {
		t.Fatalf("result does not explain runtime isolation: %q", result.Scope)
	}
	// Discovery times are instants; the snapshot is what a model subtracts them from.
	if result.At < before || result.At > time.Now().UnixMilli() {
		t.Fatalf("snapshot_at_ms %d is not the time of this listing", result.At)
	}
}
