package ax

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func asObject(t *testing.T, v any) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw(v), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func storedData(t *testing.T, b *broker, id string) (string, string) {
	t.Helper()
	var state, data string
	if err := b.db.QueryRow("SELECT state,data FROM messages WHERE id=?", id).Scan(&state, &data); err != nil {
		t.Fatal(err)
	}
	return state, data
}

func TestDeliveryStatusComputesReplacementWithoutRewritingMessage(t *testing.T) {
	b, _ := localBroker(t)
	s, from, _ := endpoint(t, b, "web", testMesh)
	r, to, _ := endpoint(t, b, "api", testMesh)
	insert := func(m Message) {
		t.Helper()
		m.Sender, m.Recipient = Agent{ID: s.ID, Name: "web", Host: "codex"}, r.ID
		m.Created, m.Expires = time.Now().UnixMilli(), time.Now().Add(time.Hour).UnixMilli()
		if _, err := b.db.Exec("INSERT INTO messages(id,sender,recipient,client_id,request_hash,seq,state,data,created,expires) VALUES(?,?,?,?,?,?,?,?,?,?)", m.ID, s.ID, r.ID, m.ID, m.ID, m.Seq, m.State, string(raw(m)), m.Created, m.Expires); err != nil {
			t.Fatal(err)
		}
		if _, err := b.db.Exec("INSERT INTO events(message,state,at,epoch) VALUES(?,?,?,1)", m.ID, m.State, m.Created); err != nil {
			t.Fatal(err)
		}
	}
	insert(Message{ID: "msg_first", Text: "old request", State: "acknowledged", Seq: 1})
	_, before := storedData(t, b, "msg_first")
	check := func(id string, want any) {
		t.Helper()
		for _, caller := range []*serverConn{from, to} {
			status := compactToolResult("ax.status", request(t, b, caller, "ax.status", object{"message_id": id}), "")
			out := asObject(t, status)
			message := out["message"].(map[string]any)
			if got := message["superseded_by"]; got != want {
				t.Fatalf("%s superseded_by = %v, want %v", id, got, want)
			}
			if id == "msg_first" && (message["status"] != "acknowledged" || out["acknowledged"] != true || out["task_completion"] != "unknown" || len(out["events"].([]any)) != 1) {
				t.Fatalf("status changed delivery evidence: %v", out)
			}
		}
		if state, after := storedData(t, b, "msg_first"); state != "acknowledged" || after != before || strings.Contains(after, "superseded_by") {
			t.Fatalf("status rewrote the earlier message: %s %s", state, after)
		}
	}
	check("msg_first", nil)
	insert(Message{ID: "msg_second", Text: "replacement", Supersedes: "msg_first", State: "queued", Seq: 2})
	check("msg_first", "msg_second")
	check("msg_second", nil)
	insert(Message{ID: "msg_third", Text: "newest replacement", Supersedes: "msg_first", State: "queued", Seq: 3})
	check("msg_first", "msg_third")
	if err := b.event("msg_third", "abandoned", 0); err != nil {
		t.Fatal(err)
	}
	check("msg_first", "msg_second")
	if err := b.event("msg_second", "expired", 0); err != nil {
		t.Fatal(err)
	}
	check("msg_first", nil)
}

// A correction names the message it replaces. The earlier message keeps its
// state and its stored row; nothing is withdrawn.
func TestReplacementLinksForwardOnly(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, _, _ = endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "add the dependency", "client_message_id": "first"}).(Message)
	_, before := storedData(t, b, first.ID)
	args := object{"target": "api", "text": "do not add the dependency; keep the current one", "client_message_id": "second", "replaces": first.ID}
	second := request(t, b, from, "ax.send", args).(Message)
	if got := asObject(t, second)["supersedes"]; got != first.ID {
		t.Fatalf("replacement does not name the earlier message: %v", got)
	}
	if state, after := storedData(t, b, first.ID); state != "queued" || after != before {
		t.Fatalf("earlier message changed: %s %s", state, after)
	}
	if again := request(t, b, from, "ax.send", args).(Message); again.ID != second.ID {
		t.Fatalf("retry stored another message: %s", again.ID)
	}
	args["replaces"] = "msg_other"
	_, err := b.request(from, "ax.send", raw(args))
	var rpc *rpcError
	if !errors.As(err, &rpc) || rpc.Code != -32003 || rpc.Data["reason"] != "key_conflict" {
		t.Fatalf("key reused with another replaces: %#v", err)
	}
	third := request(t, b, from, "ax.follow_up", object{"message_id": first.ID, "text": "here is the full plan instead", "client_message_id": "third", "replaces": first.ID}).(Message)
	if got := asObject(t, third)["supersedes"]; got != first.ID {
		t.Fatalf("follow-up does not name the earlier message: %v", got)
	}
}

// The recipient learns about the correction when it reads the earlier message:
// in its offer, through get_message, list_pending and get_thread. The link is
// computed when read and never written onto the earlier row.
func TestRecipientSeesTheReplacementWhenReading(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, to, wire := endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "build the feature", "client_message_id": "first"}).(Message)
	_, before := storedData(t, b, first.ID)
	second := request(t, b, from, "ax.send", object{"target": "api", "text": "the feature already exists; review it instead", "client_message_id": "second", "replaces": first.ID}).(Message)
	offer := make(chan packet, 1)
	go func() { p, _ := readFrame(wire); offer <- p }()
	b.dispatch()
	p := <-offer
	var offered map[string]any
	if err := json.Unmarshal(p.Params, &offered); err != nil || p.Method != "ax.delivery.offer" || offered["message_id"] != first.ID || offered["superseded_by"] != second.ID {
		t.Fatalf("offer of the earlier message: %s %s", p.Method, p.Params)
	}
	if got := asObject(t, request(t, b, to, "ax.get_message", object{"message_id": first.ID}))["superseded_by"]; got != second.ID {
		t.Fatalf("get_message: superseded_by %v", got)
	}
	found := false
	for _, item := range asObject(t, request(t, b, to, "ax.pending", object{}))["messages"].([]any) {
		entry := item.(map[string]any)
		if entry["message_id"] == first.ID {
			found = entry["superseded_by"] == second.ID
		}
	}
	if !found {
		t.Fatal("list_pending does not name the replacement")
	}
	found = false
	for _, item := range asObject(t, request(t, b, to, "ax.thread", object{"message_id": first.ID}))["messages"].([]any) {
		entry := item.(map[string]any)
		if entry["message_id"] == first.ID {
			found = entry["superseded_by"] == second.ID
		}
	}
	if !found {
		t.Fatal("get_thread does not name the replacement")
	}
	// A replacement that ends without reaching the recipient does not count.
	third := request(t, b, from, "ax.send", object{"target": "api", "text": "never mind, build it after all", "client_message_id": "third", "replaces": first.ID}).(Message)
	request(t, b, &serverConn{}, "ax.abandon", object{"message_id": third.ID})
	if got := asObject(t, request(t, b, to, "ax.get_message", object{"message_id": first.ID}))["superseded_by"]; got != second.ID {
		t.Fatalf("abandoned replacement counted: superseded_by %v", got)
	}
	if _, after := storedData(t, b, first.ID); after != before && strings.Contains(after, "superseded_by") {
		t.Fatalf("reverse link stored on the earlier row: %s", after)
	}
}

// replaces must name the caller's own live message to the same local
// recipient. Anything else is refused before any write.
func TestReplacesRefusals(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, _, _ = endpoint(t, b, "api", testMesh)
	_, stranger, _ := endpoint(t, b, "other", testMesh)
	mine := request(t, b, from, "ax.send", object{"target": "api", "text": "review", "client_message_id": "mine"}).(Message)
	elsewhere := request(t, b, from, "ax.send", object{"target": "other", "text": "review", "client_message_id": "elsewhere"}).(Message)
	theirs := request(t, b, stranger, "ax.send", object{"target": "api", "text": "review", "client_message_id": "theirs"}).(Message)
	closed := request(t, b, from, "ax.send", object{"target": "api", "text": "obsolete", "client_message_id": "closed"}).(Message)
	request(t, b, &serverConn{}, "ax.abandon", object{"message_id": closed.ID})
	var messages int
	count := func() int {
		t.Helper()
		var n int
		if err := b.db.QueryRow("SELECT count(*) FROM messages").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	messages = count()
	for _, tc := range []struct {
		name, method string
		args         object
		text         string
	}{
		{"not an ID", "ax.send", object{"target": "api", "text": "fix", "client_message_id": "k1", "replaces": "not an id"}, "message ID"},
		{"unknown", "ax.send", object{"target": "api", "text": "fix", "client_message_id": "k2", "replaces": "msg_missing"}, "not found"},
		{"another sender", "ax.send", object{"target": "api", "text": "fix", "client_message_id": "k3", "replaces": theirs.ID}, "your own earlier message to the same recipient"},
		{"another recipient", "ax.send", object{"target": "api", "text": "fix", "client_message_id": "k4", "replaces": elsewhere.ID}, "your own earlier message to the same recipient"},
		{"already ended", "ax.send", object{"target": "api", "text": "fix", "client_message_id": "k5", "replaces": closed.ID}, "already abandoned"},
		{"reply", "ax.reply", object{"message_id": theirs.ID, "text": "fix", "client_message_id": "k6", "replaces": mine.ID}, "send_message and follow_up"},
		{"resend", "ax.resend", object{"message_id": mine.ID, "client_message_id": "k7", "replaces": mine.ID}, "send_message and follow_up"},
	} {
		_, err := b.request(from, tc.method, raw(tc.args))
		var rpc *rpcError
		if !errors.As(err, &rpc) || rpc.Code != -32003 || rpc.Data["reason"] != "invalid_replaces" || !strings.Contains(rpc.Message, tc.text) {
			t.Fatalf("%s: %#v", tc.name, err)
		}
		if n := count(); n != messages {
			t.Fatalf("%s: refusal stored a message", tc.name)
		}
	}
}

// The link does not cross machines, so a replacement toward a paired machine
// is refused rather than delivered without it.
func TestReplacesStaysLocal(t *testing.T) {
	a, remote := pairedNetworkBrokers(t)
	_, from, _ := endpoint(t, a, "web", testMesh)
	api, _, _ := endpoint(t, remote, "api", testMesh)
	request(t, a, &serverConn{}, "ax.network.sync", object{"alias": "buildbox", "agents": []networkAgent{{ID: api.ID, Name: "api", Host: "codex", State: "ready"}}})
	first := request(t, a, from, "ax.send", object{"target": "api@buildbox", "text": "one", "client_message_id": "one"}).(Message)
	_, err := a.request(from, "ax.send", raw(object{"target": "api@buildbox", "text": "two", "client_message_id": "two", "replaces": first.ID}))
	var rpc *rpcError
	if !errors.As(err, &rpc) || rpc.Code != -32003 || rpc.Data["reason"] != "invalid_replaces" || !strings.Contains(rpc.Message, "paired machine") {
		t.Fatalf("replacement toward a paired machine: %#v", err)
	}
}

// Local peer mail tells the reader what to do; relayed external mail carries
// only the IDs, since its text is data and never instructions.
func TestCompactMessageNamesTheReplacement(t *testing.T) {
	decode := func(fields object) Message {
		t.Helper()
		var m Message
		if err := json.Unmarshal(raw(fields), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	earlier := compactMessage(decode(object{"message_id": "msg_a", "text": "build it", "superseded_by": "msg_b"}), true)
	if earlier["superseded_by"] != "msg_b" || !strings.HasPrefix(earlier["guidance"].(string), "Message msg_b replaces this one. Acknowledge this one and act on msg_b instead. ") {
		t.Fatalf("earlier local message: %v", earlier)
	}
	later := compactMessage(decode(object{"message_id": "msg_b", "text": "review it", "supersedes": "msg_a"}), true)
	if later["supersedes"] != "msg_a" || !strings.HasPrefix(later["guidance"].(string), "This message replaces msg_a; if you already acted on msg_a, adjust to this one. ") {
		t.Fatalf("replacement: %v", later)
	}
	relayed := compactMessage(decode(object{"message_id": "msg_c", "text": "data", "superseded_by": "msg_d", "provenance": object{"origin": "mail", "trust": "unverified"}}), true)
	if relayed["superseded_by"] != "msg_d" || strings.Contains(relayed["guidance"].(string), "msg_d") {
		t.Fatalf("relayed message: %v", relayed)
	}
	// In a chain, only the newest message asks to be acted on.
	middle := compactMessage(decode(object{"message_id": "msg_b", "text": "review it", "supersedes": "msg_a", "superseded_by": "msg_c"}), true)
	if guidance := middle["guidance"].(string); !strings.HasPrefix(guidance, "Message msg_c replaces this one. Acknowledge this one and act on msg_c instead. ") || strings.Contains(guidance, "adjust to this one") || middle["supersedes"] != "msg_a" {
		t.Fatalf("message in the middle of a chain: %v", middle)
	}
	if bare := compactMessage(decode(object{"message_id": "msg_a", "superseded_by": "msg_b"}), false); bare["superseded_by"] != "msg_b" || bare["guidance"] != nil {
		t.Fatalf("message without content: %v", bare)
	}
}

// A bridge checks the running broker before sending replaces, which an older
// broker would silently ignore. Other sends do not pay for the check.
func TestReplacesNeedsABrokerThatSupportsIt(t *testing.T) {
	dir := testDir(t)
	s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "web", Host: "claude", Mesh: testMesh, Native: uuid(), Started: true}
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	x, y := net.Pipe()
	defer x.Close()
	sent := make(chan string, 4)
	go func() {
		for {
			p, err := readFrame(x)
			if err != nil {
				return
			}
			result := object{}
			if p.Method == "ax.ping" {
				result = object{"version": "1", "local_attach": true, "relay": true, "network_mailbox": true}
			}
			if p.Method == "ax.send" {
				sent <- p.Method
				result = object{"id": "msg_x"}
			}
			if writeFrame(x, packet{ID: p.ID, Result: raw(result)}) != nil {
				return
			}
		}
	}()
	b := &bridge{ctx: context.Background(), dir: dir, file: file, session: s, c: newClient(y)}
	defer y.Close()
	args := object{"target": "api", "text": "fix", "client_message_id": "k1", "replaces": "msg_a"}
	err := b.toolCall(context.Background(), nil, "ax.send", args, nil)
	if err == nil || !strings.Contains(err.Error(), "finish the broker update") {
		t.Fatalf("replaces sent to a broker without it: %v", err)
	}
	if failure := toolFailure(err, args); failure["submission"] != "not_submitted" {
		t.Fatalf("refused before sending, yet: %v", failure)
	}
	select {
	case m := <-sent:
		t.Fatalf("%s reached the broker", m)
	default:
	}
	if err := b.toolCall(context.Background(), nil, "ax.send", object{"target": "api", "text": "plain", "client_message_id": "k2"}, nil); err != nil {
		t.Fatalf("plain send: %v", err)
	}
	if m := <-sent; m != "ax.send" {
		t.Fatal(m)
	}
}

// The reverse lookup runs whenever a message is read, so it must use its index.
func TestReplacementLookupUsesItsIndex(t *testing.T) {
	b, _ := localBroker(t)
	rows, err := b.db.Query("EXPLAIN QUERY PLAN "+successorQuery, "msg_a", "agt_a", "agt_b", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	plan := ""
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail + "\n"
	}
	if !strings.Contains(plan, "message_supersedes") {
		t.Fatalf("successor lookup does not use its index:\n%s", plan)
	}
}

// A session still running a bridge from before corrections would show the
// earlier message as current, so a replacement toward it is refused. An offline
// recipient relaunches with the installed binary.
func TestReplacesRefusesASessionThatCannotShowIt(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	s := Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: "api", Host: "codex", Mesh: testMesh, Native: uuid()}
	request(t, b, &serverConn{}, "ax.enroll", s)
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	old := &serverConn{Conn: x}
	request(t, b, old, "ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret})
	request(t, b, old, "ax.presence", object{"native_session_id": s.Native, "permission_mode": "read-only", "state": "ready"})
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "build it", "client_message_id": "first"}).(Message)
	args := object{"target": "api", "text": "review it instead", "client_message_id": "second", "replaces": first.ID}
	_, err := b.request(from, "ax.send", raw(args))
	var rpc *rpcError
	if !errors.As(err, &rpc) || rpc.Code != -32003 || rpc.Data["reason"] != "invalid_replaces" || !strings.Contains(rpc.Message, "predates corrections") {
		t.Fatalf("replacement toward a session that cannot show it: %#v", err)
	}
	b.disconnect(old)
	if second := request(t, b, from, "ax.send", args).(Message); asObject(t, second)["supersedes"] != first.ID {
		t.Fatal("replacement toward an offline recipient was not stored")
	}
}

// A queued replacement past its deadline will expire before it is offered, so
// it is not named as the message to act on.
func TestReplacementSkipsAPastDeadlineSuccessor(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, to, _ := endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "build it", "client_message_id": "first"}).(Message)
	if err := b.event(first.ID, "handoff_started", to.epoch); err != nil {
		t.Fatal(err)
	}
	second := request(t, b, from, "ax.send", object{"target": "api", "text": "review it instead", "client_message_id": "second", "replaces": first.ID}).(Message)
	if got := asObject(t, request(t, b, to, "ax.get_message", object{"message_id": first.ID}))["superseded_by"]; got != second.ID {
		t.Fatalf("live replacement not named: %v", got)
	}
	if _, err := b.db.Exec("UPDATE messages SET expires=0 WHERE id=?", second.ID); err != nil {
		t.Fatal(err)
	}
	if got := asObject(t, request(t, b, to, "ax.get_message", object{"message_id": first.ID}))["superseded_by"]; got != nil {
		t.Fatalf("replacement past its deadline named: %v", got)
	}
}

// Resending an expired replacement keeps the correction link, so the earlier
// message still names a live replacement.
func TestResendKeepsTheCorrectionLink(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, _, _ = endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "build it", "client_message_id": "first"}).(Message)
	second := request(t, b, from, "ax.send", object{"target": "api", "text": "review it instead", "client_message_id": "second", "replaces": first.ID}).(Message)
	if err := b.event(second.ID, "expired", 0); err != nil {
		t.Fatal(err)
	}
	third := request(t, b, from, "ax.resend", object{"message_id": second.ID, "client_message_id": "third"}).(Message)
	if got := asObject(t, third)["supersedes"]; got != first.ID {
		t.Fatalf("resend lost the correction link: %v", got)
	}
	if got := b.successor(first.ID, first.Sender.ID, first.Recipient); got != third.ID {
		t.Fatalf("earlier message names %q, want the resend %s", got, third.ID)
	}
}

// A resend keeps the expired attempt's correction link, so it needs the same
// recipient support as a replacement does.
func TestResendOfACorrectionNeedsARecipientThatShowsIt(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	api, current, _ := endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "build it", "client_message_id": "first"}).(Message)
	second := request(t, b, from, "ax.send", object{"target": "api", "text": "review it instead", "client_message_id": "second", "replaces": first.ID}).(Message)
	if err := b.event(second.ID, "expired", 0); err != nil {
		t.Fatal(err)
	}
	// The recipient comes back through a bridge from before corrections.
	b.disconnect(current)
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	old := &serverConn{Conn: x}
	request(t, b, old, "ax.connect", object{"version": "1", "agent_id": api.ID, "secret": api.Secret})
	args := object{"message_id": second.ID, "client_message_id": "third"}
	_, err := b.request(from, "ax.resend", raw(args))
	var rpc *rpcError
	if !errors.As(err, &rpc) || rpc.Code != -32003 || rpc.Data["reason"] != "invalid_replaces" || !strings.Contains(rpc.Message, "predates corrections") {
		t.Fatalf("resent correction toward a session that cannot show it: %#v", err)
	}
	b.disconnect(old)
	if third := request(t, b, from, "ax.resend", args).(Message); asObject(t, third)["supersedes"] != first.ID {
		t.Fatal("resend toward an offline recipient lost its link")
	}
}
