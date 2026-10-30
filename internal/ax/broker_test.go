package ax

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testDir(t *testing.T) string {
	t.Helper()
	p, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(p, 0700); e != nil {
		t.Fatal(e)
	}
	return p
}
func localBroker(t *testing.T) (*broker, string) {
	t.Helper()
	dir := testDir(t)
	b, e := openBroker(dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { b.db.Close() })
	return b, dir
}
func request(t *testing.T, b *broker, c *serverConn, m string, a any) any {
	t.Helper()
	v, e := b.request(c, m, raw(a))
	if e != nil {
		t.Fatalf("%s: %v", m, e)
	}
	return v
}
func endpoint(t *testing.T, b *broker, name, mesh string) (Session, *serverConn, net.Conn) {
	t.Helper()
	s := Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: name, Host: "codex", Mesh: mesh, Native: uuid()}
	request(t, b, &serverConn{}, "ax.enroll", s)
	x, y := net.Pipe()
	c := &serverConn{Conn: x}
	t.Cleanup(func() { x.Close(); y.Close() })
	request(t, b, c, "ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret})
	request(t, b, c, "ax.presence", object{"native_session_id": s.Native, "permission_mode": "read-only", "state": "ready"})
	request(t, b, c, "ax.ready", object{})
	return s, c, y
}

const testMesh = "123456789012345678901234"

func TestDurabilityIdempotencyAndReplyAuthorization(t *testing.T) {
	b, dir := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	to, receiver, _ := endpoint(t, b, "api", "987654321098765432109876")
	_, stranger, _ := endpoint(t, b, "other", testMesh)
	if agents := request(t, b, from, "ax.list", object{}).([]discoveredAgent); len(agents) != 3 {
		t.Fatalf("cross-directory discovery: %+v", agents)
	}
	args := object{"target": "api", "text": "literal /approve @file $(touch nope)", "client_message_id": "first", "sender": "forged"}
	m := request(t, b, from, "ax.send", args).(Message)
	if m.Sender.ID != from.agent || m.Recipient != to.ID || m.Seq != 1 {
		t.Fatal(m)
	}
	again := request(t, b, from, "ax.send", args).(Message)
	if again.ID != m.ID {
		t.Fatal("duplicate stored")
	}
	args["text"] = "changed"
	if _, e := b.request(from, "ax.send", raw(args)); e == nil {
		t.Fatal("accepted conflicting idempotency key")
	}
	for _, method := range []string{"ax.get_message", "ax.ack", "ax.reply", "ax.status"} {
		if _, e := b.request(stranger, method, raw(object{"message_id": m.ID, "text": "reply", "client_message_id": "bad"})); e == nil {
			t.Fatal("unauthorized", method)
		}
	}
	if e := b.event(m.ID, "handoff_started", receiver.epoch); e != nil {
		t.Fatal(e)
	}
	reply := request(t, b, receiver, "ax.reply", object{"message_id": m.ID, "text": "answer", "client_message_id": "reply"}).(Message)
	if reply.Recipient != from.agent || reply.Parent != m.ID || reply.Depth != 1 {
		t.Fatal(reply)
	}
	b.db.Close()
	reopened, e := openBroker(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.db.Close()
	saved, e := reopened.message(m.ID)
	if e != nil || saved.State != "acknowledged" {
		t.Fatal(saved, e)
	}
	if saved.Text != "literal /approve @file $(touch nope)" {
		t.Fatal("content changed")
	}
	var count int
	reopened.db.QueryRow("SELECT count(*) FROM messages").Scan(&count)
	if count != 2 {
		t.Fatal(count)
	}
}
func TestFIFOUncertaintyAndLeaseFencing(t *testing.T) {
	b, dir := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	to, receiver, wire := endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "one", "client_message_id": "one"}).(Message)
	second := request(t, b, from, "ax.send", object{"target": "api", "text": "two", "client_message_id": "two"}).(Message)
	offered := make(chan packet, 1)
	go func() { p, _ := readFrame(wire); offered <- p }()
	b.dispatch()
	offer := <-offered
	var msg Message
	json.Unmarshal(offer.Params, &msg)
	if msg.ID != first.ID {
		t.Fatal("wrong FIFO head")
	}
	b.disconnect(receiver)
	m, _ := b.message(first.ID)
	if m.State != "delivery_uncertain" {
		t.Fatal(m.State)
	}
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	next := &serverConn{Conn: x}
	request(t, b, next, "ax.connect", object{"version": "1", "agent_id": to.ID, "secret": to.Secret})
	if _, e := b.request(receiver, "ax.ack", raw(object{"message_id": first.ID})); e == nil {
		t.Fatal("stale acknowledgment accepted")
	}
	b.dispatch()
	m, _ = b.message(second.ID)
	if m.State != "queued" {
		t.Fatal("uncertain head did not block later message")
	}
	request(t, b, next, "ax.ack", object{"message_id": first.ID})
	request(t, b, next, "ax.ready", object{})
	go func() { p, _ := readFrame(y); offered <- p }()
	b.dispatch()
	offer = <-offered
	json.Unmarshal(offer.Params, &msg)
	if msg.ID != second.ID {
		t.Fatal("second message missing")
	}
	b.db.Close()
	again, e := openBroker(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer again.db.Close()
	m, _ = again.message(second.ID)
	if m.State != "delivery_uncertain" {
		t.Fatal("restart blindly retried handoff")
	}
}

func TestDuplicateConnectionPreservesOwnerAndExpiredLeaseCanRecover(t *testing.T) {
	b, _ := localBroker(t)
	s, owner, _ := endpoint(t, b, "web", testMesh)
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	duplicate := &serverConn{Conn: x}
	args := object{"version": "1", "agent_id": s.ID, "secret": s.Secret}
	epoch := owner.epoch
	if _, err := b.request(duplicate, "ax.connect", raw(args)); err == nil {
		t.Fatal("duplicate connection displaced a healthy owner")
	}
	if p := b.peers[s.ID]; p.conn != owner || p.epoch != epoch || !p.ready {
		t.Fatal("rejected connection changed the active lease")
	}
	request(t, b, owner, "ax.heartbeat", object{})
	b.peers[s.ID].seen = time.Now().Add(-16 * time.Second)
	request(t, b, duplicate, "ax.connect", args)
	if b.peers[s.ID].epoch != epoch+1 {
		t.Fatal("expired lease did not advance on takeover")
	}
	if _, err := b.request(owner, "ax.heartbeat", raw(object{})); err == nil {
		t.Fatal("expired owner was not fenced")
	}
	b.disconnect(owner)
	request(t, b, duplicate, "ax.heartbeat", object{})
}

func TestAcceptedHandoffDoesNotWaitForTaskAcknowledgment(t *testing.T) {
	for _, receipt := range []string{"wake_accepted", "channel_written", "content_served"} {
		t.Run(receipt, func(t *testing.T) {
			b, dir := localBroker(t)
			_, from, _ := endpoint(t, b, "web", testMesh)
			_, receiver, wire := endpoint(t, b, "api", testMesh)
			first := request(t, b, from, "ax.send", object{"target": "api", "text": "long task", "client_message_id": "first"}).(Message)
			second := request(t, b, from, "ax.send", object{"target": "api", "text": "update", "client_message_id": "second"}).(Message)
			if err := b.event(first.ID, "handoff_started", receiver.epoch); err != nil {
				t.Fatal(err)
			}
			if receipt == "content_served" {
				request(t, b, receiver, "ax.get_message", object{"message_id": first.ID})
				request(t, b, receiver, "ax.receipt", object{"message_id": first.ID, "receipt": "delivery_uncertain"})
			} else {
				request(t, b, receiver, "ax.receipt", object{"message_id": first.ID, "receipt": receipt})
			}
			offered := make(chan packet, 1)
			go func() { p, _ := readFrame(wire); offered <- p }()
			b.dispatch()
			select {
			case offer := <-offered:
				var m Message
				json.Unmarshal(offer.Params, &m)
				if m.ID != second.ID {
					t.Fatal("accepted mail was replayed or later mail was skipped")
				}
			case <-time.After(time.Second):
				t.Fatal("missing task acknowledgment blocked later mail")
			}
			b.disconnect(receiver)
			b.db.Close()
			reopened, err := openBroker(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.db.Close()
			saved, _ := reopened.message(first.ID)
			if saved.State != receipt {
				t.Fatalf("accepted delivery became uncertain after reconnect/restart: %s", saved.State)
			}
			pending, _ := reopened.message(second.ID)
			if pending.State != "delivery_uncertain" {
				t.Fatalf("unconfirmed handoff lost its replay barrier: %s", pending.State)
			}
		})
	}
}
func TestUpgradeRestoresRecordedHandoffProof(t *testing.T) {
	for _, renewed := range []bool{false, true} {
		t.Run(fmt.Sprint(renewed), func(t *testing.T) {
			b, dir := localBroker(t)
			_, from, _ := endpoint(t, b, "web", testMesh)
			_, receiver, _ := endpoint(t, b, "api", testMesh)
			m := request(t, b, from, "ax.send", object{"target": "api", "text": "task", "client_message_id": "upgrade"}).(Message)
			states := []string{"handoff_started", "content_served", "delivery_uncertain"}
			if renewed {
				states = append(states, "handoff_started", "delivery_uncertain")
			}
			for _, state := range states {
				if err := b.event(m.ID, state, receiver.epoch); err != nil {
					t.Fatal(err)
				}
			}
			b.db.Close()
			reopened, err := openBroker(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.db.Close()
			saved, _ := reopened.message(m.ID)
			want := "content_served"
			if renewed {
				want = "delivery_uncertain"
			}
			if saved.State != want {
				t.Fatalf("got %s, want %s", saved.State, want)
			}
		})
	}
}

func TestPolicyAndBounds(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	to, receiver, _ := endpoint(t, b, "api", testMesh)
	if _, e := b.request(from, "ax.send", raw(object{"target": "api", "text": strings.Repeat("x", maxText+1), "client_message_id": "size"})); e == nil {
		t.Fatal("oversize accepted")
	}
	request(t, b, &serverConn{}, "ax.policy", object{"mesh": testMesh, "target": "api", "policy": "hold"})
	m := request(t, b, from, "ax.send", object{"target": "api", "text": "held", "client_message_id": "held", "ttl_seconds": 1}).(Message)
	b.dispatch()
	saved, _ := b.message(m.ID)
	if saved.State != "queued" {
		t.Fatal(saved)
	}
	b.peers[to.ID].Policy = "accept"
	request(t, b, receiver, "ax.presence", object{"state": "blocked"})
	b.dispatch()
	saved, _ = b.message(m.ID)
	if saved.State != "queued" {
		t.Fatal("approval not held")
	}
	request(t, b, receiver, "ax.presence", object{"state": "ready", "permission_mode": "bypassPermissions"})
	b.dispatch()
	saved, _ = b.message(m.ID)
	if saved.State != "queued" {
		t.Fatal("bypass not held")
	}
	b.db.Exec("UPDATE messages SET expires=0 WHERE id=?", m.ID)
	b.dispatch()
	saved, _ = b.message(m.ID)
	if saved.State != "expired" {
		t.Fatal(saved)
	}
}
func TestFrameBoundsAndFragments(t *testing.T) {
	var buf bytes.Buffer
	want := packet{ID: raw(1), Method: "test", Params: raw(object{"text": "x"})}
	if e := writeFrame(&buf, want); e != nil {
		t.Fatal(e)
	}
	got, e := readFrame(&oneByteReader{Reader: bytes.NewReader(buf.Bytes())})
	if e != nil || got.Method != "test" {
		t.Fatal(got, e)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], maxFrame+1)
	if _, e = readFrame(bytes.NewReader(header[:])); e == nil {
		t.Fatal("oversize frame accepted")
	}
	if _, e = readFrame(bytes.NewReader(buf.Bytes()[:7])); e == nil {
		t.Fatal("truncated frame accepted")
	}
}

type oneByteReader struct{ io.Reader }

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}
func TestUnixServerAndRestart(t *testing.T) {
	// Keep the socket within the macOS sockaddr_un path limit.
	base, e := os.MkdirTemp("/tmp", "ax-test-")
	if e != nil {
		t.Skip("short socket path unavailable")
	}
	base, e = filepath.EvalSymlinks(base)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, base) }()
	var c *client
	for i := 0; i < 100; i++ {
		c, e = dial(socketPath(base))
		if e == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e != nil {
		cancel()
		t.Fatal(e)
	}
	if e = c.call("ax.ping", object{}, nil); e != nil {
		t.Fatal(e)
	}
	c.close()
	cancel()
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}
func TestRuntimeRejectsSymlink(t *testing.T) {
	base := testDir(t)
	link := filepath.Join(base, "link")
	if e := os.Symlink(base, link); e != nil {
		t.Fatal(e)
	}
	if e := privateDir(link); e == nil {
		t.Fatal("symlink accepted")
	}
}

func TestHumanRecoveryUnblocksWithoutDuplicateDelivery(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, to, wire := endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "uncertain", "client_message_id": "uncertain"}).(Message)
	second := request(t, b, from, "ax.send", object{"target": "api", "text": "next", "client_message_id": "next"}).(Message)
	if e := b.event(first.ID, "delivery_uncertain", to.epoch); e != nil {
		t.Fatal(e)
	}
	if _, e := b.request(to, "ax.abandon", raw(object{"mesh": testMesh, "message_id": first.ID})); e == nil {
		t.Fatal("adapter acquired admin recovery")
	}
	request(t, b, &serverConn{}, "ax.abandon", object{"mesh": testMesh, "message_id": first.ID})
	delivered := make(chan packet, 1)
	go func() { p, _ := readFrame(wire); delivered <- p }()
	b.dispatch()
	p := <-delivered
	var m Message
	json.Unmarshal(p.Params, &m)
	if m.ID != second.ID {
		t.Fatal("repeated uncertain delivery")
	}
	if _, e := b.request(to, "ax.receipt", raw(object{"message_id": first.ID, "receipt": "wake_accepted"})); e == nil {
		t.Fatal("late receipt resurrected abandoned message")
	}
	saved, _ := b.message(first.ID)
	if saved.State != "abandoned" {
		t.Fatal(saved.State)
	}
}

func TestRecipientSequenceSurvivesRetention(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, _, _ = endpoint(t, b, "api", testMesh)
	first := request(t, b, from, "ax.send", object{"target": "api", "text": "old", "client_message_id": "old"}).(Message)
	if _, e := b.db.Exec("DELETE FROM messages WHERE id=?", first.ID); e != nil {
		t.Fatal(e)
	}
	next := request(t, b, from, "ax.send", object{"target": "api", "text": "new", "client_message_id": "new"}).(Message)
	if next.Seq != first.Seq+1 {
		t.Fatal("recipient sequence reset after retention")
	}
}

func TestResumeWaitsForHostTools(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	to, previous, _ := endpoint(t, b, "api", testMesh)
	b.disconnect(previous)
	m := request(t, b, from, "ax.send", object{"target": "api", "text": "offline mail", "client_message_id": "resume"}).(Message)
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	current := &serverConn{Conn: x}
	request(t, b, current, "ax.connect", object{"version": "1", "agent_id": to.ID, "secret": to.Secret})
	request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": to.ID, "secret": to.Secret, "native_session_id": to.Native, "permission_mode": "default", "state": "ready"})
	b.dispatch()
	saved, _ := b.message(m.ID)
	if saved.State != "queued" {
		t.Fatal("offered before MCP tools were ready", saved.State)
	}
	for _, a := range b.list() {
		// The harness reported the session live, so this is not a peer still
		// booting; it is one whose messaging has not been activated.
		if a.ID == to.ID && a.State != "inactive" {
			t.Fatal("reported readiness before tools loaded", a.State)
		}
	}
	request(t, b, current, "ax.ready", object{})
	offered := make(chan packet, 1)
	go func() { p, _ := readFrame(y); offered <- p }()
	b.dispatch()
	var got Message
	json.Unmarshal((<-offered).Params, &got)
	if got.ID != m.ID {
		t.Fatal("queued mail missing after readiness")
	}
}

func TestDiscoveryOrderIsStableAndLeadsWithLivePeers(t *testing.T) {
	b, _ := localBroker(t)
	endpoint(t, b, "delta", testMesh)
	_, alpha, _ := endpoint(t, b, "alpha", testMesh)
	_, charlie, _ := endpoint(t, b, "charlie", testMesh)
	endpoint(t, b, "bravo", testMesh)
	b.disconnect(alpha)
	b.disconnect(charlie)
	want := []string{"bravo", "delta", "alpha", "charlie"}
	for attempt := 0; attempt < 8; attempt++ {
		var got []string
		for _, a := range b.list() {
			got = append(got, a.Name)
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("attempt %d: got %v, want %v", attempt, got, want)
		}
	}
}

func TestEnrollRefreshesMeshAfterMove(t *testing.T) {
	b, dir := localBroker(t)
	s, _, _ := endpoint(t, b, "rover", testMesh)
	moved := s
	moved.Mesh = "aaaaaaaaaaaaaaaaaaaaaaaa"
	request(t, b, &serverConn{}, "ax.enroll", moved)
	if got := b.peers[s.ID].Mesh; got != moved.Mesh {
		t.Fatalf("broker kept the mesh of the previous workspace: %q", got)
	}
	b.db.Close()
	reopened, err := openBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.db.Close()
	if got := reopened.peers[s.ID].Mesh; got != moved.Mesh {
		t.Fatalf("refreshed mesh did not persist: %q", got)
	}
}

func TestOwnerAbandonsQueuedPermissionBlockedHead(t *testing.T) {
	b, _ := localBroker(t)
	_, blocked, _ := endpoint(t, b, "watcher", testMesh)
	_, sender, _ := endpoint(t, b, "web", testMesh)
	_, recipient, wire := endpoint(t, b, "api", testMesh)
	request(t, b, blocked, "ax.presence", object{"permission_mode": "unknown", "state": "ready"})
	first := request(t, b, blocked, "ax.send", object{"target": "api", "text": "synthetic alert", "client_message_id": "alert"}).(Message)
	second := request(t, b, sender, "ax.send", object{"target": "api", "text": "real task", "client_message_id": "task"}).(Message)
	b.dispatch() // The unknown sender holds the FIFO head.
	for _, connection := range []*serverConn{blocked, sender, recipient} {
		if _, err := b.request(connection, "ax.abandon", raw(object{"message_id": first.ID})); err == nil {
			t.Fatal("bound endpoint acquired owner cancellation")
		}
	}
	request(t, b, &serverConn{}, "ax.abandon", object{"message_id": first.ID})
	saved, err := b.message(first.ID)
	if err != nil || saved.State != "abandoned" || saved.Text != first.Text {
		t.Fatalf("cancellation lost history or claimed delivery: %+v %v", saved, err)
	}
	if safe(b.peers[blocked.agent]) {
		t.Fatal("owner cancellation changed sender permissions")
	}
	delivered := make(chan packet, 1)
	go func() { p, _ := readFrame(wire); delivered <- p }()
	b.dispatch()
	select {
	case p := <-delivered:
		var m Message
		if err := json.Unmarshal(p.Params, &m); err != nil || m.ID != second.ID {
			t.Fatalf("did not deliver original real task: %+v %v", m, err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner cancellation did not release FIFO")
	}
	if _, err := b.request(&serverConn{}, "ax.abandon", raw(object{"message_id": first.ID})); err == nil {
		t.Fatal("abandoned message was not terminal")
	}
}
