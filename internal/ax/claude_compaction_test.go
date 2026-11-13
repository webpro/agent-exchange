package ax

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestClaudeCompactionKeepsLifecycleState(t *testing.T) {
	for state, event := range map[string]object{
		"busy":    {"hook_event_name": "UserPromptSubmit"},
		"blocked": {"hook_event_name": "PermissionRequest", "tool_name": "Bash"},
		"ready":   {"hook_event_name": "Stop"},
	} {
		t.Run(state, func(t *testing.T) {
			dir := startTestServer(t)
			s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "api", Host: "claude", Mesh: testMesh, Native: uuid(), Started: true}
			c := connectDiscoveryPeer(t, dir, s)
			file := filepath.Join(dir, "session.json")
			if err := saveSession(file, s); err != nil {
				t.Fatal(err)
			}
			for _, hook := range []object{event, {"hook_event_name": "SessionStart", "source": "compact"}} {
				hook["session_id"] = s.Native
				if err := Hook(dir, file, bytes.NewReader(raw(hook))); err != nil {
					t.Fatal(err)
				}
			}
			if got := discoveryList(t, c)[s.ID].State; got != state {
				t.Fatalf("compaction changed %s to %s", state, got)
			}
		})
	}
}
