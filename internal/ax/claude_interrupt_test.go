package ax

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestClaudeInterruptedNoticeTurnScope(t *testing.T) {
	b, _ := localBroker(t)
	s, from, _ := noticeSender(t, b, true)
	r, to := claudeInterruptRecipient(t, b)
	b.peers[r.ID].enrolled = time.UnixMilli(500)
	messages := map[string]Message{}
	for _, tc := range []struct {
		name    string
		offered int64
	}{{"old-fyi", 1000}, {"current", 3000}, {"during", 5000}, {"queued", 0}} {
		m := request(t, b, from, "ax.send", object{"target": "api", "text": tc.name, "client_message_id": tc.name}).(Message)
		messages[tc.name] = m
		if _, err := b.db.Exec("UPDATE messages SET created=0 WHERE id=?", m.ID); err != nil {
			t.Fatal(err)
		}
		if tc.offered > 0 {
			if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
				t.Fatal(err)
			}
			request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "channel_written"})
			if _, err := b.db.Exec("UPDATE events SET at=? WHERE message=?", tc.offered, m.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, turn := range []object{{"id": "old", "status": "completed", "at_ms": 2000}, {"id": "turn-current", "status": "inProgress", "at_ms": 4000}, {"id": "turn-current", "status": "interrupted", "at_ms": 6000}} {
		state := "ready"
		if turn["status"] == "inProgress" {
			state = "busy"
		}
		request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": r.ID, "secret": r.Secret, "native_session_id": r.Native, "state": state, "claude_turn": turn})
	}
	n, err := b.notices(s.ID)
	if err != nil || len(n) != 1 || n[0].MessageID != messages["current"].ID {
		t.Fatalf("notice escaped aborted turn: %+v %v", n, err)
	}
	b.noticeStalled(time.Now().Add(6 * time.Minute))
	n, err = b.notices(s.ID)
	if err != nil || len(n) != 3 {
		t.Fatalf("interruption was rendered as lost wake: %+v %v", n, err)
	}
	stalled := map[string]bool{}
	for _, notice := range n {
		if notice.State == "stalled" {
			if !strings.Contains(notice.Guidance, "interrupted") || strings.Contains(notice.Guidance, "lost the wake") {
				t.Fatalf("wrong stall cause: %+v", notice)
			}
			stalled[notice.MessageID] = true
		}
	}
	if len(stalled) != 2 || !stalled[messages["current"].ID] || !stalled[messages["during"].ID] {
		t.Fatalf("wrong interrupted scope stalled: %+v", n)
	}
}

func TestInterruptedNoticesSkipLiveReplacements(t *testing.T) {
	for _, host := range []string{"codex", "claude"} {
		for _, live := range []bool{false, true} {
			b, _ := localBroker(t)
			s, from, _ := noticeSender(t, b, true)
			r, to, _ := endpoint(t, b, "api", testMesh)
			r.Host = host
			b.peers[r.ID].Host = host
			b.peers[r.ID].Permission = "default"
			b.peers[r.ID].corrections = true
			if err := b.save(b.peers[r.ID]); err != nil {
				t.Fatal(err)
			}
			old := request(t, b, from, "ax.send", object{"target": "api", "text": "old", "client_message_id": "old"}).(Message)
			if err := b.event(old.ID, "handoff_started", to.epoch); err != nil {
				t.Fatal(err)
			}
			if err := b.event(old.ID, "channel_written", to.epoch); err != nil {
				t.Fatal(err)
			}
			replacement := request(t, b, from, "ax.send", object{"target": "api", "text": "new", "client_message_id": "new", "replaces": old.ID}).(Message)
			if live {
				if err := b.event(replacement.ID, "handoff_started", to.epoch); err != nil {
					t.Fatal(err)
				}
				if err := b.event(replacement.ID, "channel_written", to.epoch); err != nil {
					t.Fatal(err)
				}
			} else if _, err := b.db.Exec("UPDATE messages SET expires=0 WHERE id=?", replacement.ID); err != nil {
				t.Fatal(err)
			}
			at := time.Now().UnixMilli() + 2
			turn := object{"id": "turn-1", "status": "interrupted", "at_ms": at, "started_at_ms": at - 1}
			if host == "claude" {
				request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": r.ID, "secret": r.Secret, "native_session_id": r.Native, "state": "busy", "claude_turn": object{"id": "turn-1", "status": "inProgress", "at_ms": at - 1}})
				request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": r.ID, "secret": r.Secret, "native_session_id": r.Native, "state": "ready", "claude_turn": turn})
			} else {
				observedLifecycle(t, b, r, "ready", "idle", turn)
			}
			n, err := b.notices(s.ID)
			want := old.ID
			if live {
				want = replacement.ID
			}
			if err != nil || len(n) != 1 || n[0].MessageID != want {
				t.Fatalf("%s live=%v: wrong replacement notice %+v %v", host, live, n, err)
			}
		}
	}
}

func TestClaudeDelayedStartPreservesStalledNotice(t *testing.T) {
	b, _ := localBroker(t)
	sender, from, _ := noticeSender(t, b, true)
	s, to := claudeInterruptRecipient(t, b)
	b.peers[s.ID].enrolled = time.UnixMilli(500)
	nativeClaudeTurn(t, b, s, "finished", "inProgress", 1000)
	m := offeredAt(t, b, from, to, "mid-turn task", 2000)
	nativeClaudeTurn(t, b, s, "finished", "interrupted", 3000)
	nativeClaudeTurn(t, b, s, "finished", "inProgress", 1000)
	b.noticeStalled(time.Now().Add(6 * time.Minute))
	notices, err := b.notices(sender.ID)
	if err != nil || len(notices) != 1 || notices[0].MessageID != m.ID || notices[0].State != "stalled" {
		t.Fatalf("late start suppressed stalled notice: %+v %v", notices, err)
	}
}

func claudeTurnEnd(t *testing.T, b *broker, s Session, turn string, aborted bool) {
	t.Helper()
	last := b.peers[s.ID].claudeEndedAt
	if turn := b.peers[s.ID].claudeTurn; turn != nil {
		last = max(last, turn.At)
	}
	at := max(time.Now().UnixMilli()+2, last+2)
	args := object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "state": "busy", "claude_turn": object{"id": turn, "status": "inProgress", "at_ms": at - 1}}
	request(t, b, &serverConn{}, "ax.lifecycle", args)
	status := "completed"
	if aborted {
		status = "interrupted"
	}
	args["state"] = "ready"
	args["claude_turn"] = object{"id": turn, "status": status, "at_ms": at}
	request(t, b, &serverConn{}, "ax.lifecycle", args)
}

func TestClaudeInterruptedNotices(t *testing.T) {
	for _, state := range []string{"channel_written", "content_served"} {
		t.Run(state, func(t *testing.T) {
			b, _ := localBroker(t)
			s, from, _ := noticeSender(t, b, true)
			r, to := claudeInterruptRecipient(t, b)
			m := request(t, b, from, "ax.send", object{"target": "api", "text": "task", "client_message_id": "task"}).(Message)
			if state != "queued" {
				if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
					t.Fatal(err)
				}
				request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "channel_written"})
			}
			if state == "content_served" {
				request(t, b, to, "ax.get_message", object{"message_id": m.ID})
			}
			for _, turn := range []string{"tool-aborted", "tool-aborted", "generation-aborted"} {
				claudeTurnEnd(t, b, r, turn, true)
			}
			n, err := b.notices(s.ID)
			if err != nil || len(n) != 1 || n[0].MessageID != m.ID || n[0].State != "interrupted" || n[0].TurnID != "tool-aborted" || n[0].Delivery != state {
				t.Fatalf("wrong Claude interruption notice: %+v %v", n, err)
			}
			if strings.Contains(n[0].Guidance, "Codex") || strings.Contains(n[0].Guidance, "ax resume") {
				t.Fatalf("wrong guidance: %s", n[0].Guidance)
			}
			assertMessageState(t, b, m.ID, state)
			b.peers[s.ID].interruptedNotices = false
			if n, _ := b.notices(s.ID); len(n) != 0 {
				t.Fatalf("legacy bridge received unknown status: %+v", n)
			}
			b.peers[s.ID].interruptedNotices = true
			if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
				t.Fatal(err)
			}
			request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "channel_written"})
			claudeTurnEnd(t, b, r, "next-attempt", true)
			if n, _ := b.notices(s.ID); len(n) != 2 {
				t.Fatalf("new offer attempt did not get a notice: %+v", n)
			}
		})
	}
}

func TestClaudeInterruptedMailStillReachesStalledSweep(t *testing.T) {
	for _, aborted := range []bool{false, true} {
		b, _ := localBroker(t)
		s, from, _ := noticeSender(t, b, true)
		r, to := claudeInterruptRecipient(t, b)
		request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": r.ID, "secret": r.Secret, "native_session_id": r.Native, "state": "busy"})
		m := request(t, b, from, "ax.send", object{"target": "api", "text": "stranded", "client_message_id": "stranded"}).(Message)
		if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
			t.Fatal(err)
		}
		request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "channel_written"})
		claudeTurnEnd(t, b, r, "turn-1", aborted)
		now := time.Now().Add(time.Millisecond)
		b.peers[r.ID].stateSince = now
		b.noticeStalled(now.Add(4 * time.Minute))
		b.noticeStalled(now.Add(6 * time.Minute))
		b.noticeStalled(now.Add(7 * time.Minute))
		n, err := b.notices(s.ID)
		if !aborted {
			if err != nil || len(n) != 0 {
				t.Fatalf("normal Claude turn counted as lost wake: %+v %v", n, err)
			}
		} else if err != nil || len(n) != 2 || n[0].State != "interrupted" || n[1].State != "stalled" || !n[1].IdleObserved || n[1].Attempt != 1 {
			t.Fatalf("interrupted ready Claude was excluded from stalled sweep: %+v %v", n, err)
		}
	}
}

func TestClaudeInterruptedStallSurvivesBrokerRestart(t *testing.T) {
	b, dir := localBroker(t)
	s, from, _ := noticeSender(t, b, true)
	r, to := claudeInterruptRecipient(t, b)
	m := request(t, b, from, "ax.send", object{"target": "api", "text": "stranded", "client_message_id": "stranded"}).(Message)
	if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
		t.Fatal(err)
	}
	request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "channel_written"})
	claudeTurnEnd(t, b, r, "turn-1", true)
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
		t.Cleanup(func() { x.Close(); y.Close() })
		c := &serverConn{Conn: x}
		caps, _ := sessionCapabilities(session)
		request(t, recovered, c, "ax.connect", object{"version": "1", "agent_id": session.ID, "secret": session.Secret, "delivery_capabilities": caps, "interrupted_notices": true})
		request(t, recovered, c, "ax.ready", object{})
	}
	recovered.noticeStalled(time.Now().Add(4 * time.Minute))
	if n, _ := recovered.notices(s.ID); len(n) != 1 {
		t.Fatalf("restart skipped the idle window: %+v", n)
	}
	recovered.noticeStalled(time.Now().Add(6 * time.Minute))
	if n, _ := recovered.notices(s.ID); len(n) != 2 || n[1].State != "stalled" || n[1].MessageID != m.ID {
		t.Fatalf("restart lost interrupted attempt: %+v", n)
	}
}

func offeredAt(t *testing.T, b *broker, from, to *serverConn, key string, at int64) Message {
	t.Helper()
	m := request(t, b, from, "ax.send", object{"target": "api", "text": key, "client_message_id": strings.ReplaceAll(key, " ", "-")}).(Message)
	if _, err := b.db.Exec("UPDATE messages SET created=0 WHERE id=?", m.ID); err != nil {
		t.Fatal(err)
	}
	if err := b.event(m.ID, "handoff_started", to.epoch); err != nil {
		t.Fatal(err)
	}
	request(t, b, to, "ax.receipt", object{"message_id": m.ID, "receipt": "channel_written"})
	if _, err := b.db.Exec("UPDATE events SET at=? WHERE message=?", at, m.ID); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestClaudeSupersededCompletionRetainsIdentityAndNotice(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		b, _ := localBroker(t)
		sender, from, _ := noticeSender(t, b, true)
		s, to := claudeInterruptRecipient(t, b)
		b.peers[s.ID].enrolled = time.UnixMilli(500)
		m := offeredAt(t, b, from, to, "first task", 1000)
		nativeClaudeTurn(t, b, s, "first", "inProgress", 2000)
		nativeClaudeTurn(t, b, s, "next", "inProgress", 3000)
		if blocked {
			request(t, b, &serverConn{}, "ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "state": "blocked", "state_cause": object{"kind": "permission", "tool": "Bash"}})
		}
		nativeClaudeTurn(t, b, s, "first", "interrupted", 4000)
		want := "busy"
		if blocked {
			want = "blocked"
		}
		if p := b.peers[s.ID]; p.State != want || p.claudeTurn == nil || p.claudeTurn.ID != "next" {
			t.Fatalf("old turn changed current state: %s, %+v", p.State, p.claudeTurn)
		}
		n, err := b.notices(sender.ID)
		if err != nil || len(n) != 1 || n[0].MessageID != m.ID || n[0].TurnID != "first" {
			t.Fatalf("superseded start lost its notice: %+v %v", n, err)
		}
	}
}

func TestClaudeMidTurnMailWaitsForWholeReadyWindow(t *testing.T) {
	for _, continues := range []bool{false, true} {
		b, _ := localBroker(t)
		sender, from, _ := noticeSender(t, b, true)
		s, to := claudeInterruptRecipient(t, b)
		b.peers[s.ID].enrolled = time.UnixMilli(500)
		nativeClaudeTurn(t, b, s, "first", "inProgress", 1000)
		m := offeredAt(t, b, from, to, "mid turn", 2000)
		nativeClaudeTurn(t, b, s, "first", "interrupted", 3000)
		if n, _ := b.notices(sender.ID); len(n) != 0 {
			t.Fatalf("mid-turn mail notified immediately: %+v", n)
		}
		now := time.Now()
		b.peers[s.ID].stateSince = now
		b.noticeStalled(now.Add(4 * time.Minute))
		if n, _ := b.notices(sender.ID); len(n) != 0 {
			t.Fatalf("short idle window notified: %+v", n)
		}
		if continues {
			nativeClaudeTurn(t, b, s, "next", "inProgress", 4000)
		}
		b.noticeStalled(now.Add(6 * time.Minute))
		n, err := b.notices(sender.ID)
		if continues {
			if err != nil || len(n) != 0 {
				t.Fatalf("continuing queue notified: %+v %v", n, err)
			}
		} else if err != nil || len(n) != 1 || n[0].MessageID != m.ID || n[0].State != "stalled" || n[0].TurnID != "first" {
			t.Fatalf("stopped mid-turn mail stayed silent: %+v %v", n, err)
		}
	}
}

func TestClaudeDeferredFetchedMailSurvivesRestart(t *testing.T) {
	b, dir := localBroker(t)
	sender, from, _ := noticeSender(t, b, true)
	s, to := claudeInterruptRecipient(t, b)
	b.peers[s.ID].enrolled = time.UnixMilli(500)
	nativeClaudeTurn(t, b, s, "first", "inProgress", 1000)
	m := offeredAt(t, b, from, to, "fetched-mid", 2000)
	request(t, b, to, "ax.get_message", object{"message_id": m.ID})
	if _, err := b.db.Exec("UPDATE events SET at=2000 WHERE message=?", m.ID); err != nil {
		t.Fatal(err)
	}
	nativeClaudeTurn(t, b, s, "first", "interrupted", 3000)
	if n, _ := b.notices(sender.ID); len(n) != 0 {
		t.Fatalf("fetched mail notified immediately: %+v", n)
	}
	if err := b.db.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := openBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { recovered.db.Close() })
	for _, session := range []Session{sender, s} {
		x, y := net.Pipe()
		t.Cleanup(func() { x.Close(); y.Close() })
		c := &serverConn{Conn: x}
		caps, _ := sessionCapabilities(session)
		request(t, recovered, c, "ax.connect", object{"version": "1", "agent_id": session.ID, "secret": session.Secret, "delivery_capabilities": caps, "interrupted_notices": true})
		request(t, recovered, c, "ax.ready", object{})
	}
	recovered.noticeStalled(time.Now().Add(4 * time.Minute))
	if n, _ := recovered.notices(sender.ID); len(n) != 0 {
		t.Fatalf("restart skipped idle window: %+v", n)
	}
	recovered.noticeStalled(time.Now().Add(6 * time.Minute))
	n, err := recovered.notices(sender.ID)
	if err != nil || len(n) != 1 || n[0].State != "stalled" || n[0].Delivery != "content_served" || n[0].TurnID != "first" || strings.Contains(n[0].Guidance, "not been fetched") {
		t.Fatalf("deferred fetched evidence lost: %+v %v", n, err)
	}
}

func TestClaudeDeferredOfferCanBeNoticedOnItsNextTurn(t *testing.T) {
	b, _ := localBroker(t)
	sender, from, _ := noticeSender(t, b, true)
	s, to := claudeInterruptRecipient(t, b)
	b.peers[s.ID].enrolled = time.UnixMilli(500)
	nativeClaudeTurn(t, b, s, "first", "inProgress", 1000)
	m := offeredAt(t, b, from, to, "mid", 2000)
	nativeClaudeTurn(t, b, s, "first", "interrupted", 3000)
	nativeClaudeTurn(t, b, s, "second", "inProgress", 4000)
	nativeClaudeTurn(t, b, s, "second", "interrupted", 5000)
	n, err := b.notices(sender.ID)
	if err != nil || len(n) != 1 || n[0].TurnID != "second" {
		t.Fatalf("no immediate notice when the next turn stopped: %+v %v", n, err)
	}
	var turn string
	var deferred bool
	if err := b.db.QueryRow("SELECT turn_id,deferred FROM interrupted_offers WHERE message=? AND attempt=1", m.ID).Scan(&turn, &deferred); err != nil || turn != "second" || deferred {
		t.Fatalf("fact and notice diverged: %s %v %v", turn, deferred, err)
	}
}

func TestClaudeLateCompletionNarrowsTheNextTurnScope(t *testing.T) {
	b, _ := localBroker(t)
	sender, from, _ := noticeSender(t, b, true)
	s, to := claudeInterruptRecipient(t, b)
	b.peers[s.ID].enrolled = time.UnixMilli(500)
	nativeClaudeTurn(t, b, s, "first", "inProgress", 1000)
	offeredAt(t, b, from, to, "handled by the first turn", 1500)
	next := offeredAt(t, b, from, to, "waiting for the next turn", 2500)
	nativeClaudeTurn(t, b, s, "next", "inProgress", 3000)
	nativeClaudeTurn(t, b, s, "first", "completed", 2000)
	nativeClaudeTurn(t, b, s, "next", "interrupted", 4000)
	if n, err := b.notices(sender.ID); err != nil || len(n) != 1 || n[0].MessageID != next.ID {
		t.Fatalf("mail from a completed turn was reported as interrupted: %+v %v", n, err)
	}
}
