package ax

import (
	"encoding/json"
	"testing"
	"time"
)

func codexUsageEvent(thread string, used, window any) object {
	return object{"method": "thread/tokenUsage/updated", "params": object{"threadId": thread, "turnId": "turn-1", "tokenUsage": object{
		"last": object{"totalTokens": used}, "total": object{"totalTokens": 9876543}, "modelContextWindow": window,
	}}}
}

func TestCodexContextThroughNativeProxy(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	back := f.connect("active")
	f.waitState("busy")
	f.send(back, codexUsageEvent(observedThread, 32000, 128000))
	f.status(back, observedThread, "idle")
	deadline := time.Now().Add(3 * time.Second)
	for {
		a := f.waitState("ready")
		var fields object
		json.Unmarshal(raw(a), &fields)
		if fields["context_used_tokens"] == float64(32000) && fields["context_window_tokens"] == float64(128000) && fields["context_used_percent"] == float64(25) {
			if at, ok := fields["context_observed_at_ms"].(float64); !ok || at <= 0 {
				t.Fatalf("missing observation time: %v", fields)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("native last.totalTokens never reached discovery: %v", fields)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCodexContextDoesNotInventLifecycle(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	back := f.connect("")
	f.waitState("ready")
	f.send(back, codexUsageEvent(observedThread, 8000, nil))
	deadline := time.Now().Add(3 * time.Second)
	for {
		var agents []discoveredAgent
		if err := f.admin.call("ax.inspect", object{}, &agents); err != nil {
			t.Fatal(err)
		}
		if len(agents) == 1 && agents[0].ContextUsage != nil && agents[0].UsedTokens != nil {
			if agents[0].State != "ready" || *agents[0].UsedTokens != 8000 {
				t.Fatalf("usage changed unknown lifecycle: %+v", agents[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("usage never arrived")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCodexContextFilteringAndCompaction(t *testing.T) {
	o := codexSessionObserver{pending: map[string]bool{}}
	if got := o.response(raw(codexUsageEvent(observedThread, 32000, 128000))); got != nil {
		t.Fatalf("usage bound an unselected thread: %s", got)
	}
	o.native, o.status = observedThread, "idle"
	if got := o.response(raw(codexUsageEvent("other-thread", 32000, 128000))); got != nil {
		t.Fatalf("foreign usage reached AX: %s", got)
	}
	decode := func(data []byte) object {
		t.Helper()
		var fields struct {
			Usage object `json:"context_usage"`
		}
		if err := json.Unmarshal(data, &fields); err != nil || fields.Usage == nil {
			t.Fatalf("missing usage: %s %v", data, err)
		}
		return fields.Usage
	}
	u := decode(o.response(raw(codexUsageEvent(observedThread, 32000, 128000))))
	if u["context_used_tokens"] != float64(32000) || u["context_window_tokens"] != float64(128000) {
		t.Fatalf("used cumulative totals or lost window: %v", u)
	}
	u = decode(o.response(raw(object{"method": "item/started", "params": object{"threadId": observedThread, "item": object{"type": "contextCompaction", "id": "compact-1"}}})))
	if _, ok := u["context_used_tokens"]; ok || u["context_window_tokens"] != float64(128000) {
		t.Fatalf("compaction retained a stale count: %v", u)
	}
	u = decode(o.response(raw(codexUsageEvent(observedThread, 8000, nil))))
	if u["context_used_tokens"] != float64(8000) {
		t.Fatalf("post-compaction observation missing: %v", u)
	}
	if _, ok := u["context_window_tokens"]; ok {
		t.Fatalf("missing window was guessed: %v", u)
	}
	for _, used := range []any{-1, 1.5, "bad", 9007199254740992} {
		if got := o.response(raw(codexUsageEvent(observedThread, used, 128000))); got != nil {
			t.Fatalf("invalid usage reached AX: %s", got)
		}
	}
	u = decode(o.unavailable())
	if u["context_used_tokens"] != float64(8000) {
		t.Fatalf("lifecycle coalescing lost the last reading: %v", u)
	}
}
