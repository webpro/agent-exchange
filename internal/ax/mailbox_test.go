package ax

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSendReceiptsRefreshWithoutChangingIdentity(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	s, to, _ := endpoint(t, b, "api", testMesh)
	p := b.peers[s.ID]
	args := object{"target": "api", "text": "review", "client_message_id": "stable"}
	var id string
	for _, state := range []string{"offline", "starting", "busy", "ready"} {
		p.conn, p.ready, p.State = to, true, state
		if state == "offline" {
			p.conn = nil
		} else if state == "starting" {
			p.ready = false
		}
		m := request(t, b, from, "ax.send", args).(Message)
		if id == "" {
			id = m.ID
		}
		r := m.Receipt
		if m.ID != id || r == nil || r.ClientID != "stable" || r.Recipient.State != state || r.Recipient.Online != (state != "offline") || r.Queued != 1 || r.Pending != 1 || r.Acknowledged || r.TaskCompletion != "unknown" {
			t.Fatalf("wrong %s snapshot: %+v", state, r)
		}
		if (state == "offline" || state == "starting") && !strings.Contains(r.Evidence, state) {
			t.Fatalf("missing queue explanation: %+v", r)
		}
		stored, err := b.message(id)
		if err != nil || stored.Receipt != nil {
			t.Fatalf("transient receipt persisted: %+v %v", stored, err)
		}
	}
	for _, state := range []string{"wake_accepted", "channel_written", "content_served", "acknowledged"} {
		if err := b.event(id, state, to.epoch); err != nil {
			t.Fatal(err)
		}
		m := request(t, b, from, "ax.send", args).(Message)
		if m.ID != id || m.State != state || m.Receipt.Acknowledged != (state == "acknowledged") || m.Receipt.TaskCompletion != "unknown" || m.Receipt.Queued != 0 {
			t.Fatalf("receipt confused delivery and completion: %+v", m)
		}
		if state == "wake_accepted" && !strings.Contains(m.Receipt.Evidence, "has not acknowledged") {
			t.Fatal("native acceptance claimed recipient acknowledgment")
		}
		status := request(t, b, from, "ax.status", object{"message_id": id}).(object)
		if status["acknowledged"] != (state == "acknowledged") || status["task_completion"] != "unknown" || status["delivery_evidence"] != m.Receipt.Evidence {
			t.Fatalf("status and send receipt disagree: %+v", status)
		}
	}
	answer := request(t, b, to, "ax.reply", object{"message_id": id, "text": "reviewed", "client_message_id": "answer", "ttl_seconds": 172800}).(Message)
	if answer.Receipt.Recipient.ID != from.agent || answer.Receipt.ClientID != "answer" || answer.Expires-answer.Created != 172800000 {
		t.Fatalf("reply lost receipt or expiry: %+v", answer)
	}
}

func TestMessageTTLValidationAndRetention(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, to, _ := endpoint(t, b, "api", testMesh)
	for _, value := range []string{"null", "0", "-1", "1.5", `"60"`, "true", "{}", "[]", "604801", "999999999999999999999"} {
		for _, method := range []string{"ax.send", "ax.reply", "ax.resend", "ax.follow_up"} {
			args := object{"target": "api", "message_id": "unused", "text": "invalid", "client_message_id": "bad", "ttl_seconds": json.RawMessage(value)}
			if _, err := b.request(from, method, raw(args)); err == nil || !strings.Contains(err.Error(), "ttl_seconds") {
				t.Fatalf("%s accepted TTL %s: %v", method, value, err)
			}
		}
	}
	var count int
	if err := b.db.QueryRow("SELECT count(*) FROM messages").Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid expiry queued mail: %d %v", count, err)
	}
	for _, ttl := range []int{0, 1, 172800, maxTTL} {
		args := object{"target": "api", "text": fmt.Sprint(ttl), "client_message_id": fmt.Sprint(ttl)}
		want := defaultTTL
		if ttl != 0 {
			args["ttl_seconds"], want = ttl, ttl
		}
		m := request(t, b, from, "ax.send", args).(Message)
		if m.Expires-m.Created != int64(want)*1000 {
			t.Fatalf("wrong effective expiry: %+v", m)
		}
		args["ttl_seconds"] = want + 1
		if want == maxTTL {
			args["ttl_seconds"] = want - 1
		}
		if _, err := b.request(from, "ax.send", raw(args)); err == nil || !strings.Contains(err.Error(), "different content") {
			t.Fatalf("changed TTL reused idempotency key: %v", err)
		}
		if ttl == maxTTL {
			b.cleanup(time.UnixMilli(m.Created).Add(6 * 24 * time.Hour))
			if _, err := b.message(m.ID); err != nil {
				t.Fatal("cleanup removed a live multi-day request")
			}
		}
	}
	// Use an expired deadline without sleeping. Dispatch still owns expiry and
	// creates the sender's durable failure notification, even for held mail.
	b.peers[to.agent].Policy = "hold"
	if _, err := b.db.Exec("UPDATE messages SET expires=0"); err != nil {
		t.Fatal(err)
	}
	b.dispatch()
	if notices := request(t, b, from, "ax.notifications", object{}).([]deliveryNotice); len(notices) != 4 {
		t.Fatalf("expiry failed to report all requests: %+v", notices)
	}
}

func TestPendingDiscoveryPreservesStateAndRecipientIsolation(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	s, to, _ := endpoint(t, b, "api", testMesh)
	_, stranger, _ := endpoint(t, b, "other", testMesh)
	states := []string{"wake_accepted", "content_served", "channel_written", "handoff_started", "queued", "acknowledged", "expired", "refused", "abandoned"}
	var ids []string
	for i, state := range states {
		m := request(t, b, from, "ax.send", object{"target": "api", "text": fmt.Sprint(i) + strings.Repeat("界", 120), "client_message_id": fmt.Sprint(i)}).(Message)
		ids = append(ids, m.ID)
		if state != "queued" {
			if err := b.event(m.ID, state, to.epoch); err != nil {
				t.Fatal(err)
			}
		}
	}
	b.disconnect(to) // In-flight handoff becomes uncertain, never replayed.
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	to = &serverConn{Conn: x}
	request(t, b, to, "ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret})
	request(t, b, to, "ax.ready", object{})
	var before, after int
	b.db.QueryRow("SELECT count(*) FROM events").Scan(&before)
	page := request(t, b, to, "ax.pending", object{"target": stranger.agent}).(pendingPage)
	if len(page.Messages) != 5 || page.Next != 0 || dispatchAfter("ax.pending") {
		t.Fatalf("listing includes terminal mail or triggers dispatch: %+v", page)
	}
	for i, m := range page.Messages {
		if m.ID != ids[i] || m.Acknowledged || m.Age < 0 {
			t.Fatalf("wrong pending message: %+v", m)
		}
		if i < 4 && len([]rune(m.Preview)) != 100 {
			t.Fatalf("offered preview is not bounded: %+v", m)
		}
	}
	if page.Messages[3].State != "delivery_uncertain" || page.Messages[4].BlockedBy != ids[3] || !strings.Contains(page.Messages[4].Evidence, "uncertain") || page.Messages[4].Preview != "" {
		t.Fatalf("queued mail bypassed uncertain FIFO head: %+v", page)
	}
	if _, err := b.request(to, "ax.get_message", raw(object{"message_id": ids[4]})); err == nil {
		t.Fatal("pending discovery made queued content fetchable")
	}
	for _, policy := range []string{"hold", "refuse", "accept"} {
		b.peers[s.ID].Policy = policy
		if policy == "accept" {
			b.peers[s.ID].Permission = "unknown"
		}
		hidden := request(t, b, to, "ax.pending", object{}).(pendingPage)
		for _, m := range hidden.Messages {
			if m.Preview != "" {
				t.Fatal("listing bypassed delivery controls")
			}
		}
	}
	b.db.QueryRow("SELECT count(*) FROM events").Scan(&after)
	if before != after {
		t.Fatal("listing mutated delivery receipts")
	}
	if page := request(t, b, stranger, "ax.pending", object{"target": s.ID}).(pendingPage); len(page.Messages) != 0 {
		t.Fatal("cross-recipient mail leaked")
	}
	if _, err := b.request(&serverConn{}, "ax.pending", raw(object{})); err == nil {
		t.Fatal("unauthenticated pending discovery")
	}
}

func TestPendingPaginationUsesStableRecipientSequences(t *testing.T) {
	b, _ := localBroker(t)
	_, to, _ := endpoint(t, b, "api", testMesh)
	for sender := 0; sender < 3; sender++ {
		_, from, _ := endpoint(t, b, fmt.Sprintf("sender%d", sender), testMesh)
		for i := 0; i < 20; i++ {
			request(t, b, from, "ax.send", object{"target": "api", "text": fmt.Sprint(i), "client_message_id": fmt.Sprint(i)})
		}
	}
	first := request(t, b, to, "ax.pending", object{}).(pendingPage)
	if len(first.Messages) != 50 || first.Next != 50 {
		t.Fatalf("unbounded first page: %+v", first)
	}
	if err := b.event(first.Messages[0].ID, "acknowledged", to.epoch); err != nil {
		t.Fatal(err)
	}
	second := request(t, b, to, "ax.pending", object{"after_seq": first.Next}).(pendingPage)
	if len(second.Messages) != 10 || second.Messages[0].Seq != 51 || second.Messages[9].Seq != 60 || second.Next != 0 {
		t.Fatalf("receipt change shifted pagination: %+v", second)
	}
	for _, cursor := range []any{nil, -1, 1.5, "50", true, maxSequence + 1} {
		if _, err := b.request(to, "ax.pending", raw(object{"after_seq": cursor})); err == nil {
			t.Fatalf("accepted malformed cursor: %v", cursor)
		}
	}
}

func TestMCPForwardsTypedExpiryAndPendingArguments(t *testing.T) {
	dir := startTestServer(t)
	admin, err := dial(socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.close()
	s := Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: "web", Host: "claude", Mesh: testMesh, Native: uuid(), Started: true}
	to := s
	to.ID, to.Name = randomID("agt_"), "api"
	for _, session := range []Session{s, to} {
		if err := admin.call("ax.enroll", session, nil); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	var in, out bytes.Buffer
	for i, call := range []object{
		{"name": "send_message", "arguments": object{"target": "api", "text": "two days", "ttl_seconds": 172800, "client_message_id": "mcp_send"}},
		{"name": "list_pending", "arguments": object{"after_seq": 0, "target": to.ID}},
		{"name": "send_message", "arguments": object{"target": "api", "text": "invalid", "ttl_seconds": 0.5}},
		{"name": "list_pending", "arguments": object{"after_seq": -1}},
		{"name": "resend_message", "arguments": object{"message_id": "missing", "client_message_id": "resend_mcp"}},
		{"name": "follow_up", "arguments": object{"message_id": "missing", "text": "addendum", "client_message_id": "followup_mcp"}},
		{"name": "get_thread", "arguments": object{"message_id": "missing", "after_message_id": "unused"}},
	} {
		json.NewEncoder(&in).Encode(packet{JSONRPC: "2.0", ID: raw(i + 1), Method: "tools/call", Params: raw(call)})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := Bridge(ctx, dir, file, &in, &out); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&out)
	for i := 0; i < 7; i++ {
		var frame packet
		var response struct {
			IsError bool                    `json:"isError"`
			Content []struct{ Text string } `json:"content"`
		}
		if err := dec.Decode(&frame); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(frame.Result, &response); err != nil || response.IsError != (i >= 2) || len(response.Content) != 1 {
			t.Fatalf("bad MCP response: %s %v", frame.Result, err)
		}
		if i == 0 {
			var result struct{ Message Message }
			if err := json.Unmarshal([]byte(response.Content[0].Text), &result); err != nil || result.Message.Expires-result.Message.Created != 172800000 || result.Message.Receipt == nil || result.Message.Receipt.ClientID != "mcp_send" {
				t.Fatalf("MCP stripped expiry or receipt: %+v %v", result, err)
			}
		}
		if i == 4 {
			var failure object
			if err := json.Unmarshal([]byte(response.Content[0].Text), &failure); err != nil || failure["client_message_id"] != "resend_mcp" || failure["message"] != "message not found" {
				t.Fatalf("resend did not reach broker with stable key: %s", response.Content[0].Text)
			}
		}
		if i == 5 || i == 6 {
			var failure object
			if err := json.Unmarshal([]byte(response.Content[0].Text), &failure); err != nil || failure["message"] != "message not found" {
				t.Fatal("thread tool did not reach broker")
			}
			if i == 5 && failure["client_message_id"] != "followup_mcp" {
				t.Fatal("follow-up lost retry key")
			}
		}
		if i == 1 {
			var page pendingPage
			if err := json.Unmarshal([]byte(response.Content[0].Text), &page); err != nil || len(page.Messages) != 0 {
				t.Fatal("MCP accepted forged pending recipient")
			}
		}
	}
}

func TestSnapshotSeparatesNeverStartedFromClosedAndInactive(t *testing.T) {
	b, _ := localBroker(t)
	s, conn, _ := endpoint(t, b, "api", testMesh)
	p := b.peers[s.ID]
	for _, c := range []struct {
		name       string
		connected  bool
		lifecycled bool
		activated  bool
		want       string
		reason     string
	}{
		{"enrolled but never started", false, false, false, "unstarted", "never started"},
		{"ran before and is now closed", false, true, true, "offline", "offline"},
		{"connected and still booting", true, false, false, "starting", "starting"},
		{"running but messaging never activated", true, true, false, "inactive", "not activated AX messaging"},
		{"running and reachable", true, true, true, "ready", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			p.conn, p.seen, p.ready = nil, time.Now(), c.activated
			if c.connected {
				p.conn = conn
			}
			p.State = "starting"
			if c.lifecycled {
				p.State = "ready"
			}
			if got := agentSnapshot(p).State; got != c.want {
				t.Fatalf("reported %q, want %q", got, c.want)
			}
			if c.reason == "" {
				return
			}
			if got := queueReason(p, p); !strings.Contains(got, c.reason) {
				t.Fatalf("queue reason %q does not explain %q", got, c.reason)
			}
		})
	}
}

func TestPermissionBlockedDiscoveryAndRecovery(t *testing.T) {
	for _, blockedSide := range []string{"recipient", "sender"} {
		t.Run(blockedSide, func(t *testing.T) {
			b, _ := localBroker(t)
			_, from, _ := endpoint(t, b, "web", testMesh)
			_, to, wire := endpoint(t, b, "reviewer", testMesh)
			blocked := b.peers[to.agent]
			if blockedSide == "sender" {
				blocked = b.peers[from.agent]
			}
			blocked.Host, blocked.Permission = "grok", "bypassPermissions"
			m := request(t, b, from, "ax.send", object{"target": "reviewer", "text": "private review request", "client_message_id": "held"}).(Message)
			b.dispatch()
			for _, a := range request(t, b, from, "ax.list", object{}).([]discoveredAgent) {
				if a.ID == blocked.ID && (a.State != "permission-blocked" || !a.Online) {
					t.Fatalf("blocked peer advertised as reachable: %+v", a)
				}
			}
			if !strings.Contains(strings.ToLower(m.Receipt.Evidence), blockedSide) || !strings.Contains(m.Receipt.Evidence, "AX_ALLOW_BYPASS=1") || !strings.Contains(m.Receipt.Evidence, blocked.Name) {
				t.Fatalf("missing actionable permission reason: %+v", m.Receipt)
			}
			page := request(t, b, to, "ax.pending", object{}).(pendingPage)
			if len(page.Messages) != 1 || page.Messages[0].Preview != "" || page.Messages[0].State != "queued" || !strings.Contains(page.Messages[0].Evidence, "AX_ALLOW_BYPASS=1") {
				t.Fatalf("pending listing leaked or lost held request: %+v", page)
			}
			if _, err := b.request(to, "ax.get_message", raw(object{"message_id": m.ID})); err == nil {
				t.Fatal("unoffered content was readable")
			}
			// An intentional AX opt-in clears the diagnostic without altering
			// native permissions; returning to a supported mode also clears it.
			blocked.AllowBypass = true
			if agentSnapshot(blocked).State != "ready" || blocked.Permission != "bypassPermissions" {
				t.Fatal("opt-in changed native permissions or retained the block")
			}
			blocked.AllowBypass, blocked.Permission = false, "default"
			if agentSnapshot(blocked).State != "ready" {
				t.Fatal("permission recovery remained blocked")
			}
			wire.SetDeadline(time.Now().Add(time.Second))
			offered := make(chan packet, 1)
			go func() { p, _ := readFrame(wire); offered <- p }()
			b.dispatch()
			var delivered Message
			json.Unmarshal((<-offered).Params, &delivered)
			if delivered.ID != m.ID {
				t.Fatalf("recovery failed to hand off the original request: %+v", delivered)
			}
		})
	}
}
