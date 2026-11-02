package ax

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A keyed write refused before any write carries code -32003 with a reason and
// a recovery step. The message text is unchanged and the mailbox keeps its rows.
func TestSendRefusalsAreTaggedAndWriteNothing(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, to, _ := endpoint(t, b, "api", testMesh)
	_, _, _ = endpoint(t, b, "quiet", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "review", "client_message_id": "first"}).(Message)
	closed := request(t, b, from, "ax.send", object{"target": "api", "text": "obsolete", "client_message_id": "closed"}).(Message)
	request(t, b, &serverConn{}, "ax.abandon", object{"message_id": closed.ID})
	request(t, b, &serverConn{}, "ax.policy", object{"target": "quiet", "policy": "refuse"})
	count := func() (int, int) {
		t.Helper()
		var m, e int
		if err := b.db.QueryRow("SELECT (SELECT count(*) FROM messages),(SELECT count(*) FROM events)").Scan(&m, &e); err != nil {
			t.Fatal(err)
		}
		return m, e
	}
	messages, events := count()
	for _, tc := range []struct {
		name, method string
		caller       *serverConn
		args         object
		reason, text string
	}{
		{"unknown target", "ax.send", from, object{"target": "nobody", "text": "hello", "client_message_id": "k1"}, "unknown_target", "not found in this AX runtime"},
		{"key conflict", "ax.send", from, object{"target": "api", "text": "changed", "client_message_id": "first"}, "key_conflict", "different content"},
		{"bad key", "ax.send", from, object{"target": "api", "text": "hello", "client_message_id": "not a key"}, "invalid_client_message_id", "client_message_id"},
		{"empty text", "ax.send", from, object{"target": "api", "text": "", "client_message_id": "k2"}, "invalid_text", "UTF-8 text"},
		{"ttl", "ax.send", from, object{"target": "api", "text": "hello", "client_message_id": "k3", "ttl_seconds": 0}, "invalid_ttl", "ttl_seconds must be"},
		{"wrong type", "ax.send", from, object{"target": 7, "text": "hello", "client_message_id": "k4"}, "invalid_parameters", "invalid parameters"},
		{"self", "ax.send", from, object{"target": "web", "text": "hello", "client_message_id": "k5"}, "invalid_target", "invalid recipient"},
		{"refusing recipient", "ax.send", from, object{"target": "quiet", "text": "hello", "client_message_id": "k6"}, "recipient_refuses", "refuses messages"},
		{"repeated text", "ax.send", from, object{"target": "api", "text": "review", "client_message_id": "k7"}, "repeated_text", "repeated message suppressed"},
		{"resend before expiry", "ax.resend", from, object{"message_id": first.ID, "client_message_id": "k8"}, "not_expired", "only expired"},
		{"resend by another", "ax.resend", to, object{"message_id": first.ID, "client_message_id": "k9"}, "not_sender", "only the original sender"},
		{"resend unknown", "ax.resend", from, object{"message_id": "msg_missing", "client_message_id": "k10"}, "unknown_message", "message not found"},
		{"reply by the sender", "ax.reply", from, object{"message_id": first.ID, "text": "me", "client_message_id": "k11"}, "not_recipient", "only the recipient"},
		{"follow-up by the recipient", "ax.follow_up", to, object{"message_id": first.ID, "text": "me", "client_message_id": "k12"}, "not_sender", "only the sender may follow up"},
		{"reply before the offer", "ax.reply", to, object{"message_id": first.ID, "text": "early", "client_message_id": "k13"}, "not_offered", "not eligible"},
		{"follow-up after abandonment", "ax.follow_up", from, object{"message_id": closed.ID, "text": "late", "client_message_id": "k14"}, "already_terminal", "not eligible"},
	} {
		_, err := b.request(tc.caller, tc.method, raw(tc.args))
		var rpc *rpcError
		if !errors.As(err, &rpc) || rpc.Code != -32003 || rpc.Data["reason"] != tc.reason {
			t.Fatalf("%s: not a tagged refusal: %#v", tc.name, err)
		}
		if !strings.Contains(rpc.Message, tc.text) {
			t.Fatalf("%s: message text changed: %q", tc.name, rpc.Message)
		}
		if recovery, _ := rpc.Data["recovery"].(string); recovery == "" {
			t.Fatalf("%s: no recovery step: %v", tc.name, rpc.Data)
		}
		if m, e := count(); m != messages || e != events {
			t.Fatalf("%s: refusal wrote rows: messages %d -> %d, events %d -> %d", tc.name, messages, m, events, e)
		}
	}
	// The rate limit is the one refusal worth retrying, after the window it names.
	for i := 0; ; i++ {
		if i > 40 {
			t.Fatal("rate limit never reached")
		}
		_, err := b.request(from, "ax.send", raw(object{"target": "api", "text": fmt.Sprint("bulk ", i), "client_message_id": fmt.Sprint("bulk", i)}))
		if err == nil {
			continue
		}
		var rpc *rpcError
		if !errors.As(err, &rpc) || rpc.Code != -32003 || rpc.Data["reason"] != "rate_limit" {
			t.Fatalf("rate limit refusal: %#v", err)
		}
		if wait, _ := rpc.Data["retry_after_ms"].(int64); wait < 1 || wait > 60000 {
			t.Fatalf("rate limit wait: %v", rpc.Data["retry_after_ms"])
		}
		break
	}
}

// Owner commands and reads keep the generic code: -32003 belongs to the keyed
// writes, whose bridges turn it into a definite non-send.
func TestRefusalCodeStaysOnKeyedWrites(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	for _, tc := range []struct {
		name, method string
		caller       *serverConn
		args         object
		text         string
	}{
		{"owner policy for an unknown agent", "ax.policy", &serverConn{}, object{"target": "nobody", "policy": "hold"}, "not found in this AX runtime"},
		{"read with a wrong type", "ax.get_message", from, object{"message_id": 7}, "invalid parameters"},
	} {
		_, err := b.request(tc.caller, tc.method, raw(tc.args))
		if err == nil || !strings.Contains(err.Error(), tc.text) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		var rpc *rpcError
		if errors.As(err, &rpc) && rpc.Code == -32003 {
			t.Fatalf("%s: tagged outside the keyed writes: %#v", tc.name, rpc)
		}
	}
}

// Over the broker wire, a refused keyed write is a definite non-send: not
// submitted, not retryable, and a recovery that says nothing was sent and what
// to change. Only the recovery and a rate limit's wait reach the tool result.
func TestRefusalReportsNotSubmittedOverWire(t *testing.T) {
	dir := startTestServer(t)
	c, err := dial(socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	call := func(method string, args, result any) {
		t.Helper()
		if err := c.call(method, args, result); err != nil {
			t.Fatal(err)
		}
	}
	s := Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: "web", Host: "claude", Mesh: testMesh, Native: uuid(), Started: true}
	call("ax.enroll", s, nil)
	target := s
	target.ID, target.Name, target.Native = randomID("agt_"), "api", uuid()
	call("ax.enroll", target, nil)
	call("ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret}, nil)
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	var first Message
	call("ax.send", object{"target": "api", "text": "review", "client_message_id": "first"}, &first)
	bridge := &bridge{ctx: context.Background(), dir: dir, file: file, session: s, c: c}
	for _, tc := range []struct {
		name string
		args object
		want string
	}{
		{"unknown target", object{"target": "nobody", "text": "hello", "client_message_id": "k1"}, "list_agents"},
		{"key conflict", object{"target": "api", "text": "changed", "client_message_id": "first"}, "new client_message_id"},
	} {
		err := bridge.toolCall(context.Background(), nil, "ax.send", tc.args, nil)
		if err == nil {
			t.Fatalf("%s: send accepted", tc.name)
		}
		failure := toolFailure(err, tc.args)
		if failure["code"] != "request_refused" || failure["submission"] != "not_submitted" || failure["retryable"] != false || failure["client_message_id"] != tc.args["client_message_id"] {
			t.Fatalf("%s: refusal not reported as a definite non-send: %v", tc.name, failure)
		}
		recovery, _ := failure["recovery"].(string)
		if !strings.HasPrefix(recovery, "Nothing was sent. ") || !strings.Contains(recovery, tc.want) || strings.Contains(recovery, "reuse this client_message_id") {
			t.Fatalf("%s: wrong recovery: %q", tc.name, recovery)
		}
		if _, ok := failure["reason"]; ok {
			t.Fatalf("%s: broker data beyond the allowlist reached the tool result: %v", tc.name, failure)
		}
	}
	keyed := object{"client_message_id": "k2"}
	limited := toolFailure(&requestFailure{cause: &rpcError{Code: -32003, Message: "send rate limit reached", Data: object{"reason": "rate_limit", "recovery": "Wait a minute, then send again with the same arguments.", "retry_after_ms": float64(1500)}}, submitted: true}, keyed)
	if limited["code"] != "request_refused" || limited["submission"] != "not_submitted" || limited["retryable"] != true || limited["retry_after_ms"] != float64(1500) {
		t.Fatalf("rate limit refusal: %v", limited)
	}
	bare := toolFailure(&requestFailure{cause: &rpcError{Code: -32003, Message: "refused"}, submitted: true}, keyed)
	if bare["recovery"] != "Nothing was sent. Change the request as the error message says, then send again." {
		t.Fatalf("refusal without a broker step: %v", bare)
	}
	// Transport failures after submission remain uncertain, with the same-key advice.
	lost := toolFailure(&requestFailure{cause: &transportError{cause: errors.New("broker connection closed"), submitted: true}, submitted: true}, keyed)
	if lost["code"] != "broker_unavailable" || lost["submission"] != "outcome_unknown" || lost["retryable"] != true || !strings.Contains(lost["recovery"].(string), "reuse this client_message_id") {
		t.Fatalf("transport failure lost its uncertainty: %v", lost)
	}
	// So does an untagged broker error, which may be a failed commit.
	commit := toolFailure(&requestFailure{cause: &rpcError{Code: -32000, Message: "commit failed"}, submitted: true}, keyed)
	if commit["code"] != "request_failed" || commit["submission"] != "outcome_unknown" {
		t.Fatalf("untagged broker error treated as a definite non-send: %v", commit)
	}
	// A tool without a key keeps its report.
	read := toolFailure(&requestFailure{cause: &rpcError{Code: -32003, Message: "refused"}, submitted: true}, object{"message_id": "msg_x"})
	if read["code"] != "request_failed" || read["submission"] != "not_applicable" {
		t.Fatalf("keyless refusal: %v", read)
	}
}

// Once an attempt was submitted and lost its response, a refusal of the
// automatic retry cannot prove that nothing was sent, so the send stays
// outcome_unknown with the same-key advice.
func TestRefusalAfterLostResponseStaysUncertain(t *testing.T) {
	store, dir := localBroker(t)
	s, owner, wire := endpoint(t, store, "web", testMesh)
	_, _, _ = endpoint(t, store, "api", testMesh)
	args := object{"target": "nobody", "text": "hello", "client_message_id": "stable"}
	s.Host, s.Started = "claude", true
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	bridge := &bridge{ctx: context.Background(), dir: dir, file: file, session: s, c: newClient(wire)}
	defer func() {
		bridge.mu.Lock()
		c := bridge.c
		bridge.mu.Unlock()
		if c != nil {
			c.close()
		}
	}()
	done := make(chan error, 1)
	go func() {
		dropped := false
		for {
			p, err := readFrame(owner)
			if err != nil {
				done <- err
				return
			}
			if p.Method == "ax.send" && !dropped {
				// Lose the first attempt before the broker answers, and let the
				// bridge retry on a fresh connection.
				dropped = true
				owner.Close()
				store.disconnect(owner)
				x, y := net.Pipe()
				owner = &serverConn{Conn: x}
				defer owner.Close()
				store.mu.Lock()
				_, err = store.request(owner, "ax.connect", raw(object{"version": "1", "agent_id": s.ID, "secret": s.Secret}))
				store.mu.Unlock()
				if err != nil {
					y.Close()
					done <- err
					return
				}
				bridge.mu.Lock()
				bridge.c = newClient(y)
				bridge.changedLocked()
				bridge.mu.Unlock()
				continue
			}
			store.mu.Lock()
			value, err := store.request(owner, p.Method, p.Params)
			store.mu.Unlock()
			response := packet{ID: p.ID, Result: raw(value)}
			if err != nil {
				response.Result = nil
				response.Error = &rpcError{Code: -32000, Message: err.Error()}
				var rpc *rpcError
				if errors.As(err, &rpc) {
					response.Error = rpc
				}
			}
			if err = writeFrame(owner, response); err != nil {
				done <- err
				return
			}
			if p.Method == "ax.send" {
				done <- nil
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := bridge.toolCall(ctx, nil, "ax.send", args, nil)
	if err == nil {
		t.Fatal("retry accepted")
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	var rpc *rpcError
	if !errors.As(err, &rpc) || rpc.Code != -32003 {
		t.Fatalf("retry was not refused: %#v", err)
	}
	failure := toolFailure(err, args)
	recovery, _ := failure["recovery"].(string)
	if failure["code"] != "request_failed" || failure["submission"] != "outcome_unknown" || !strings.Contains(recovery, "reuse this client_message_id") || strings.Contains(recovery, "Nothing was") {
		t.Fatalf("refusal after a lost attempt: %v", failure)
	}
}
