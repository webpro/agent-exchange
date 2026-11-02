package ax

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

func noticeSender(t *testing.T, b *broker, supported bool) (Session, *serverConn, net.Conn) {
	t.Helper()
	s, previous, _ := endpoint(t, b, "web", testMesh)
	b.disconnect(previous)
	x, y := net.Pipe()
	c := &serverConn{Conn: x}
	t.Cleanup(func() { x.Close(); y.Close() })
	request(t, b, c, "ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret, "interrupted_notices": supported})
	request(t, b, c, "ax.ready", object{})
	return s, c, y
}

func observedLifecycle(t *testing.T, b *broker, s Session, state, status string, turn any) {
	t.Helper()
	args := object{"agent_id": s.ID, "secret": s.Secret, "launch_id": s.LaunchID, "native_session_id": s.Native, "state": state, "codex_status": status}
	if m, ok := turn.(object); ok && m["status"] == "interrupted" {
		args["codex_interrupted"] = turn
	}
	request(t, b, &serverConn{}, "ax.lifecycle", args)
}

func TestCodexInterruptedUnresolvedMail(t *testing.T) {
	for _, state := range []string{"queued", "wake_accepted", "content_served"} {
		t.Run(state, func(t *testing.T) {
			b, _ := localBroker(t)
			s, from, _ := noticeSender(t, b, true)
			r, to, _ := endpoint(t, b, "api", testMesh)
			b.peers[r.ID].capabilities, _ = sessionCapabilities(r)
			observedLifecycle(t, b, r, "busy", "active", nil)
			m := request(t, b, from, "ax.send", object{"target": "api", "text": "review PR", "client_message_id": "interrupt"}).(Message)
			if state != "queued" {
				if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
					t.Fatal(err)
				}
				request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "wake_accepted"})
			}
			if state == "content_served" {
				request(t, b, to, "ax.get_message", object{"message_id": m.ID})
			}
			at := time.Now().UnixMilli()
			turn := object{"id": "turn-1", "status": "interrupted", "at_ms": at}
			observedLifecycle(t, b, r, "ready", "idle", turn)
			observedLifecycle(t, b, r, "ready", "idle", turn)
			notices, err := b.notices(s.ID)
			if err != nil || len(notices) != 1 {
				t.Fatalf("interrupted %s mail needs one immediate notice: %+v %v", state, notices, err)
			}
			n := notices[0]
			var wire object
			json.Unmarshal(raw(n), &wire)
			if n.State != "interrupted" || n.MessageID != m.ID || n.Recipient != "api" || n.Preview != "review PR" || wire["turn_id"] != "turn-1" || n.Delivery != state {
				t.Fatalf("wrong interruption evidence: %+v", wire)
			}
			if !strings.Contains(n.Guidance, "interrupted") || strings.Contains(n.Guidance, "queue/start") || strings.Contains(n.Guidance, "ax resume") {
				t.Fatalf("bad interrupted guidance: %q", n.Guidance)
			}
			var saved string
			if err := b.db.QueryRow("SELECT data FROM notifications WHERE id=?", n.ID).Scan(&saved); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(saved, "guidance") {
				t.Fatalf("stored notice prose: %s", saved)
			}
			assertMessageState(t, b, m.ID, state)
			later := request(t, b, from, "ax.send", object{"target": "api", "text": "later", "client_message_id": "later"}).(Message)
			if _, err := b.db.Exec("UPDATE messages SET created=? WHERE id=?", at+1, later.ID); err != nil {
				t.Fatal(err)
			}
			observedLifecycle(t, b, r, "ready", "idle", turn)
			if got, _ := b.notices(s.ID); len(got) != 1 {
				t.Fatalf("repeated snapshot attached a past interrupt to new mail: %+v", got)
			}
			b.cleanup(time.Now().Add(6 * time.Minute))
			got, _ := b.notices(s.ID)
			if state == "wake_accepted" {
				if len(got) != 2 || got[1].State != "stalled" || got[1].MessageID != m.ID {
					t.Fatalf("T4 disappeared after adding idle proof: %+v", got)
				}
			} else if len(got) != 1 {
				t.Fatalf("non-offered mail incorrectly reported stalled: %+v", got)
			}
			assertMessageState(t, b, m.ID, state)
		})
	}
}

func TestCodexInterruptedNoticeRequiresSenderSupport(t *testing.T) {
	b, _ := localBroker(t)
	s, from, wire := noticeSender(t, b, false)
	r, _, _ := endpoint(t, b, "api", testMesh)
	m := request(t, b, from, "ax.send", object{"target": "api", "text": "interrupted", "client_message_id": "interrupted"}).(Message)
	observedLifecycle(t, b, r, "ready", "idle", object{"id": "turn-1", "status": "interrupted", "at_ms": time.Now().UnixMilli()})
	var count int
	b.db.QueryRow("SELECT count(*) FROM notifications WHERE message=?", m.ID).Scan(&count)
	if count != 1 {
		t.Fatalf("interruption fact was not retained for an older sender: %d", count)
	}
	if n, _ := b.notices(s.ID); len(n) != 0 {
		t.Fatalf("old bridge received unsupported notice: %+v", n)
	}
	other := request(t, b, from, "ax.send", object{"target": "api", "text": "expires", "client_message_id": "expires"}).(Message)
	if err := b.event(other.ID, "expired", 0); err != nil {
		t.Fatal(err)
	}
	frames := make(chan packet, 1)
	go func() { p, _ := readFrame(wire); frames <- p }()
	b.dispatchNotice(b.peers[s.ID], time.Now())
	p := <-frames
	var n deliveryNotice
	if json.Unmarshal(p.Params, &n) != nil || n.State != "expired" || n.MessageID != other.ID {
		t.Fatalf("unsupported notice starved a terminal notice: %+v", p)
	}
}

func TestCodexFakeServerInterruptedTurn(t *testing.T) {
	f := newCodexLifecycleFixture(t)
	back := f.connect("idle")
	f.waitState("ready")
	f.send(back, object{"method": "turn/started", "params": object{"threadId": observedThread, "turn": object{"id": "turn-1", "status": "inProgress"}}})
	f.waitState("busy")
	f.send(back, object{"method": "turn/completed", "params": object{"threadId": observedThread, "turn": object{"id": "turn-1", "status": "interrupted"}}})
	f.waitState("ready")
	// A backend that provides turn events but no thread/status events is observable too.
	f.send(back, object{"method": "turn/started", "params": object{"threadId": "22222222-2222-4222-8222-222222222222", "turn": object{"id": "other-turn", "status": "inProgress"}}})
	f.waitState("ready")
}

func TestCodexFakeServerInterruptNotifiesSender(t *testing.T) {
	for _, fetched := range []bool{false, true} {
		name := "before-fetch"
		if fetched {
			name = "after-fetch"
		}
		t.Run(name, func(t *testing.T) {
			f := newCodexLifecycleFixture(t)
			back := f.connect("active")
			f.waitState("busy")
			from := f.sender()
			var message Message
			if err := from.call("ax.send", object{"target": "api", "text": "review PR", "client_message_id": "interrupt"}, &message); err != nil {
				t.Fatal(err)
			}
			select {
			case offer := <-f.bridge.offers:
				if err := f.bridge.call("ax.receipt", object{"message_id": offer.ID, "receipt": "wake_accepted", "handoff_id": offer.HandoffID}, nil); err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("mail was held while busy")
			}
			if fetched {
				if err := f.bridge.call("ax.get_message", object{"message_id": message.ID}, nil); err != nil {
					t.Fatal(err)
				}
			}
			f.send(back, object{"method": "turn/completed", "params": object{"threadId": observedThread, "turn": object{"id": "interrupted-turn", "status": "interrupted"}}})
			f.waitState("ready")
			select {
			case n := <-from.notices:
				if n.State != "interrupted" || n.MessageID != message.ID || !strings.Contains(n.Guidance, "interrupted") {
					t.Fatalf("missing native interrupt wake: %+v", n)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("native interruption did not notify the sender")
			}
			// The proxy observes only. No native request may appear after selection.
			back.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
			var p packet
			if err := back.ReadJSON(&p); err == nil {
				t.Fatalf("observer issued a native request: %+v", p)
			}
		})
	}
}

func TestCodexInterruptSurvivesLaterSnapshots(t *testing.T) {
	o := codexSessionObserver{pending: map[string]bool{}}
	o.request(raw(object{"id": 1, "method": "thread/resume"}))
	o.response(raw(object{"id": 1, "result": object{"thread": object{"id": observedThread, "status": object{"type": "active"}}}}))
	var latest []byte
	for _, event := range []struct{ method, id, status string }{
		{"turn/started", "turn-1", "inProgress"},
		{"turn/completed", "turn-1", "interrupted"},
		{"turn/started", "turn-2", "inProgress"},
		{"turn/completed", "turn-2", "completed"},
	} {
		latest = o.response(raw(object{"method": event.method, "params": object{"threadId": observedThread, "turn": object{"id": event.id, "status": event.status}}}))
	}
	var fields object
	if json.Unmarshal(latest, &fields) != nil {
		t.Fatalf("bad snapshot: %s", latest)
	}
	_, current := fields["codex_turn"]
	interrupted, _ := fields["codex_interrupted"].(map[string]any)
	if fields["codex_status"] != "idle" || current || interrupted["id"] != "turn-1" || interrupted["status"] != "interrupted" {
		t.Fatalf("coalescing lost or changed interruption evidence: %s", latest)
	}
}

func TestCodexInterruptReplayAfterBrokerRestart(t *testing.T) {
	b, dir := localBroker(t)
	s, from, _ := noticeSender(t, b, true)
	r, to, _ := endpoint(t, b, "api", testMesh)
	b.peers[r.ID].capabilities, _ = sessionCapabilities(r)
	observedLifecycle(t, b, r, "busy", "active", nil)
	m := request(t, b, from, "ax.send", object{"target": "api", "text": "interrupted", "client_message_id": "interrupted"}).(Message)
	if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
		t.Fatal(err)
	}
	request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "wake_accepted"})
	turn := object{"id": "turn-1", "status": "interrupted", "at_ms": time.Now().UnixMilli()}
	observedLifecycle(t, b, r, "ready", "idle", turn)
	if err := b.db.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := openBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { recovered.db.Close() })
	for _, session := range []Session{s, r} {
		x, y := net.Pipe()
		c := &serverConn{Conn: x}
		t.Cleanup(func() { x.Close(); y.Close() })
		caps, _ := sessionCapabilities(session)
		request(t, recovered, c, "ax.connect", object{"version": "1", "agent_id": session.ID, "secret": session.Secret, "delivery_capabilities": caps, "interrupted_notices": true})
		request(t, recovered, c, "ax.ready", object{})
	}
	observedLifecycle(t, recovered, r, "ready", "idle", turn)
	if n, _ := recovered.notices(s.ID); len(n) != 1 || n[0].State != "interrupted" {
		t.Fatalf("replayed interruption duplicated or lost its notice: %+v", n)
	}
	recovered.cleanup(time.Now().Add(6 * time.Minute))
	if n, _ := recovered.notices(s.ID); len(n) != 2 || n[1].State != "stalled" {
		t.Fatalf("broker restart lost T4 evidence: %+v", n)
	}
}

func TestCodexInterruptExcludesTerminalAndRelayedMail(t *testing.T) {
	b, _ := localBroker(t)
	s, from, _ := noticeSender(t, b, true)
	r, to, _ := endpoint(t, b, "api", testMesh)
	for _, state := range []string{"acknowledged", "expired", "refused", "abandoned"} {
		m := request(t, b, from, "ax.send", object{"target": "api", "text": state, "client_message_id": state}).(Message)
		if err := b.event(m.ID, state, to.epoch); err != nil {
			t.Fatal(err)
		}
	}
	relay, relayConn := relayEndpoint(t, b, "relay")
	openExternal(t, b, r, "verified")
	request(t, b, relayConn, "ax.send", object{"target": "api", "text": "external", "client_message_id": "relay", "provenance": object{"origin": "test", "trust": "verified", "tainted": false}})
	observedLifecycle(t, b, r, "ready", "idle", object{"id": "turn-1", "status": "interrupted", "at_ms": time.Now().UnixMilli()})
	var count int
	if err := b.db.QueryRow("SELECT count(*) FROM notifications WHERE json_extract(data,'$.status')='interrupted' AND recipient IN (?,?)", s.ID, relay.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("interrupted notice included terminal or relayed mail: %d %v", count, err)
	}
}

func TestCodexInterruptNoticesArePerOfferAttempt(t *testing.T) {
	b, _ := localBroker(t)
	s, from, _ := noticeSender(t, b, true)
	r, to, _ := endpoint(t, b, "api", testMesh)
	var messages []Message
	for _, state := range []string{"queued", "wake_accepted", "content_served"} {
		m := request(t, b, from, "ax.send", object{"target": "api", "text": state, "client_message_id": state}).(Message)
		if state != "queued" {
			if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
				t.Fatal(err)
			}
			request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "wake_accepted"})
		}
		if state == "content_served" {
			request(t, b, to, "ax.get_message", object{"message_id": m.ID})
		}
		messages = append(messages, m)
	}
	for _, turn := range []string{"turn-1", "turn-2", "turn-3", "turn-4"} {
		observedLifecycle(t, b, r, "busy", "active", nil)
		observedLifecycle(t, b, r, "ready", "idle", object{"id": turn, "status": "interrupted", "at_ms": time.Now().UnixMilli()})
	}
	notices, err := b.notices(s.ID)
	if err != nil || len(notices) != 3 {
		t.Fatalf("three messages across four interrupts must produce three notices: %+v %v", notices, err)
	}
	for _, n := range notices {
		if n.State != "interrupted" {
			t.Fatalf("unexpected status: %+v", n)
		}
		request(t, b, from, "ax.ack_notification", object{"notification_id": n.ID})
	}
	observedLifecycle(t, b, r, "ready", "idle", object{"id": "turn-5", "status": "interrupted", "at_ms": time.Now().UnixMilli()})
	if n, _ := b.notices(s.ID); len(n) != 0 {
		t.Fatalf("later interruption repeated acknowledged notices: %+v", n)
	}
	if err := b.event(messages[1].ID, "handoff_started", to.epoch); err != nil {
		t.Fatal(err)
	}
	request(t, b, to, "ax.receipt", object{"message_id": messages[1].ID, "receipt": "wake_accepted"})
	observedLifecycle(t, b, r, "ready", "idle", object{"id": "turn-6", "status": "interrupted", "at_ms": time.Now().UnixMilli()})
	if n, _ := b.notices(s.ID); len(n) != 1 || n[0].MessageID != messages[1].ID || n[0].Attempt != 2 {
		t.Fatalf("a new offer attempt did not get its own notice: %+v", n)
	}
	assertMessageState(t, b, messages[1].ID, "wake_accepted")
	assertMessageState(t, b, messages[2].ID, "content_served")
}

func TestCodexReplayedInterruptSkipsLaterOffers(t *testing.T) {
	b, _ := localBroker(t)
	s, from, _ := noticeSender(t, b, true)
	r, to, _ := endpoint(t, b, "api", testMesh)
	b.peers[r.ID].capabilities, _ = sessionCapabilities(r)
	observedLifecycle(t, b, r, "busy", "active", nil)
	m := request(t, b, from, "ax.send", object{"target": "api", "text": "queued before the interrupt", "client_message_id": "later"}).(Message)
	at := time.Now().UnixMilli()
	turn := object{"id": "turn-1", "status": "interrupted", "at_ms": at}
	observedLifecycle(t, b, r, "ready", "idle", turn)
	if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
		t.Fatal(err)
	}
	request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "wake_accepted"})
	if _, err := b.db.Exec("UPDATE events SET at=? WHERE message=?", at+1000, m.ID); err != nil {
		t.Fatal(err)
	}
	// A restarted broker has forgotten which turn it reported; the launcher replays it.
	b.peers[r.ID].interruptedNoticed = ""
	observedLifecycle(t, b, r, "ready", "idle", turn)
	if n, err := b.notices(s.ID); err != nil || len(n) != 1 || n[0].Attempt != 0 {
		t.Fatalf("replay reported an offer made after the interruption: %+v %v", n, err)
	}
}
