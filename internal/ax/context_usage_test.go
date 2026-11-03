package ax

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestContextUsageDiscovery(t *testing.T) {
	b, _ := localBroker(t)
	s, c, _ := endpoint(t, b, "api", testMesh)
	report := func(usage object) {
		t.Helper()
		request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "context_usage": usage})
	}
	report(object{"context_used_tokens": 32000, "context_window_tokens": 128000, "context_observed_at_ms": 1000})
	for _, result := range []any{b.discovery(), compactToolResult("ax.list", b.discovery(), "")} {
		a := listedAgent(t, result, "api")
		for key, want := range map[string]float64{"context_used_tokens": 32000, "context_window_tokens": 128000, "context_used_percent": 25, "context_observed_at_ms": 1000} {
			if a[key] != want {
				t.Fatalf("%s: want %v, got %v", key, want, a)
			}
		}
	}
	endpoint(t, b, "web", testMesh)
	m := request(t, b, c, "ax.send", object{"target": "web", "text": "ready", "client_message_id": "ready"}).(Message)
	for table, id := range map[string]string{"agents": s.ID, "messages": m.ID} {
		var stored string
		if err := b.db.QueryRow("SELECT data FROM "+table+" WHERE id=?", id).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(stored, "context_") {
			t.Fatalf("%s stored a reading: %s", table, stored)
		}
	}
	var out bytes.Buffer
	printAgents(&out, b.discovery(), time.UnixMilli(2000))
	if !strings.Contains(out.String(), "context=32000/128000(25.00%) context_seen=1s") {
		t.Fatalf("CLI lost context facts: %s", out.String())
	}
	report(object{"context_used_tokens": 0, "context_observed_at_ms": 2000})
	a := listedAgent(t, request(t, b, c, "ax.list", object{}), "api")
	if a["context_used_tokens"] != float64(0) {
		t.Fatalf("reported zero must survive: %v", a)
	}
	for _, key := range []string{"context_window_tokens", "context_used_percent"} {
		if _, ok := a[key]; ok {
			t.Fatalf("missing native window must omit %s: %v", key, a)
		}
	}
	report(object{"context_window_tokens": 128000, "context_observed_at_ms": 3000})
	report(object{"context_used_tokens": 64000, "context_window_tokens": 128000, "context_observed_at_ms": 2500})
	a = listedAgent(t, compactToolResult("ax.list", b.discovery(), ""), "api")
	if _, ok := a["context_used_tokens"]; ok || a["context_observed_at_ms"] != float64(3000) || a["context_window_tokens"] != float64(128000) {
		t.Fatalf("stale observation restored invalidated count: %v", a)
	}
	old := struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}{}
	if err := json.Unmarshal(raw(a), &old); err != nil || old.Name != "api" || old.State != "ready" {
		t.Fatalf("additive facts confused an older client: %+v %v", old, err)
	}
	request(t, b, &serverConn{}, "ax.enroll", s)
	a = listedAgent(t, b.discovery(), "api")
	if _, ok := a["context_observed_at_ms"]; ok {
		t.Fatalf("relaunch retained a prior observation: %v", a)
	}
}

func TestContextUsageInvalidObservations(t *testing.T) {
	b, _ := localBroker(t)
	s, _, _ := endpoint(t, b, "api", testMesh)
	for _, usage := range []object{
		{"context_used_tokens": -1, "context_observed_at_ms": 1000},
		{"context_window_tokens": 0, "context_observed_at_ms": 1000},
		{"context_used_tokens": 100},
		{"context_used_tokens": 9007199254740992, "context_observed_at_ms": 1000},
	} {
		request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "state": "busy", "context_usage": usage})
		a := listedAgent(t, b.discovery(), "api")
		if _, ok := a["context_observed_at_ms"]; ok || a["state"] != "busy" {
			t.Fatalf("invalid optional usage affected lifecycle: %v", a)
		}
	}
}
