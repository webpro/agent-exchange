package ax

import (
	"fmt"
	"strings"
	"testing"
)

func expiredRequest(t *testing.T, b *broker, from *serverConn, key, text string) Message {
	t.Helper()
	m := request(t, b, from, "ax.send", object{"target": "api", "text": text, "client_message_id": key, "ttl_seconds": 1}).(Message)
	if _, err := b.db.Exec("UPDATE messages SET expires=0 WHERE id=?", m.ID); err != nil {
		t.Fatal(err)
	}
	b.peers[m.Recipient].Policy = "hold"
	// This fixture has no reader for delivery-status wakes. Keep the sender
	// disconnected from dispatch while expiring the request, then restore it.
	b.peers[from.agent].ready = false
	b.dispatch()
	b.peers[from.agent].ready = true
	b.peers[m.Recipient].Policy = "accept"
	got, err := b.message(m.ID)
	if err != nil || got.State != "expired" {
		t.Fatalf("not expired: %+v %v", got, err)
	}
	return got
}

func TestResendPreservesContentHistoryAndIdempotency(t *testing.T) {
	b, dir := localBroker(t)
	s, from, _ := endpoint(t, b, "web", testMesh)
	_, to, _ := endpoint(t, b, "api", testMesh)
	original := expiredRequest(t, b, from, "original", strings.Repeat("界", maxText/3))
	args := object{"message_id": original.ID, "client_message_id": "retry", "ttl_seconds": 172800, "target": "forged", "text": "forged"}
	resent := request(t, b, from, "ax.resend", args).(Message)
	if resent.ID == original.ID || resent.ResendOf != original.ID || resent.Text != original.Text || resent.Recipient != original.Recipient || resent.Sender.ID != from.agent || resent.Seq != original.Seq+1 || resent.Expires-resent.Created != 172800000 {
		t.Fatalf("incorrect attempt: %+v", resent)
	}
	pending := request(t, b, to, "ax.pending", object{}).(pendingPage)
	if len(pending.Messages) != 1 || pending.Messages[0].ResendOf != original.ID {
		t.Fatalf("missing retry link: %+v", pending)
	}
	// A retry must still return this attempt even after the recipient accepts it
	// or its policy changes. It must not create or dispatch another copy.
	if err := b.event(resent.ID, "acknowledged", to.epoch); err != nil {
		t.Fatal(err)
	}
	b.peers[to.agent].Policy = "refuse"
	if again := request(t, b, from, "ax.resend", args).(Message); again.ID != resent.ID || again.State != "acknowledged" {
		t.Fatal(again)
	}
	saved, err := b.message(original.ID)
	if err != nil || saved.State != "expired" || saved.Text != original.Text {
		t.Fatal("original changed", err)
	}
	args["ttl_seconds"] = 60
	if _, err = b.request(from, "ax.resend", raw(args)); err == nil {
		t.Fatal("changed expiry reused key")
	}
	args["ttl_seconds"] = 172800
	b.db.Close()
	reopened, err := openBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.db.Close()
	again, err := reopened.send(reopened.peers[s.ID], "", original.ID, "", "retry", 172800, "ax.resend", "")
	if err != nil || again.(Message).ID != resent.ID {
		t.Fatalf("restart duplicated attempt: %v %v", again, err)
	}
	var count int
	if err = reopened.db.QueryRow("SELECT count(*) FROM messages").Scan(&count); err != nil || count != 2 {
		t.Fatalf("count=%d %v", count, err)
	}
}

func TestResendRejectsNonExpiredAndForeignMessages(t *testing.T) {
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, to, _ := endpoint(t, b, "api", testMesh)
	_, stranger, _ := endpoint(t, b, "other", testMesh)
	m := expiredRequest(t, b, from, "original", "request")
	args := object{"message_id": m.ID, "client_message_id": "resend"}
	for _, caller := range []*serverConn{to, stranger, {}} {
		if _, err := b.request(caller, "ax.resend", raw(args)); err == nil {
			t.Fatal("unauthorized resend")
		}
	}
	for _, state := range []string{"queued", "handoff_started", "delivery_uncertain", "wake_accepted", "channel_written", "content_served", "acknowledged", "refused", "abandoned"} {
		if err := b.event(m.ID, state, to.epoch); err != nil {
			t.Fatal(err)
		}
		if _, err := b.request(from, "ax.resend", raw(args)); err == nil {
			t.Fatalf("resent %s", state)
		}
	}
	var count int
	b.db.QueryRow("SELECT count(*) FROM messages").Scan(&count)
	if count != 1 {
		t.Fatal("rejection stored a new attempt")
	}
}

func TestResendRetainsLimitsAndReplyContext(t *testing.T) {
	for _, control := range []string{"refuse", "hold", "permissions", "rate", "capacity"} {
		t.Run(control, func(t *testing.T) {
			b, _ := localBroker(t)
			_, from, _ := endpoint(t, b, "web", testMesh)
			_, to, _ := endpoint(t, b, "api", testMesh)
			m := expiredRequest(t, b, from, "original", "request")
			switch control {
			case "refuse", "hold":
				b.peers[to.agent].Policy = control
			case "permissions":
				b.peers[to.agent].Permission = "unknown"
			case "rate":
				for i := 0; i < 29; i++ {
					request(t, b, from, "ax.send", object{"target": "api", "text": fmt.Sprint(i), "client_message_id": fmt.Sprint(i)})
				}
			case "capacity":
				_, err := b.db.Exec(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1024)
     INSERT INTO messages SELECT 'full_'||x,sender,recipient,'full_'||x,'hash',seq+x,'queued',data,0,expires FROM messages,n WHERE id=?`, m.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err := b.request(from, "ax.resend", raw(object{"message_id": m.ID, "client_message_id": "resend"}))
			if control == "hold" || control == "permissions" {
				if err != nil {
					t.Fatal(err)
				}
				b.dispatch()
				saved, err := b.message(got.(Message).ID)
				if err != nil || saved.State != "queued" {
					t.Fatal("resend bypassed delivery controls", err)
				}
			} else if err == nil {
				t.Fatalf("bypassed %s", control)
			}
		})
	}
	b, _ := localBroker(t)
	_, from, _ := endpoint(t, b, "web", testMesh)
	_, to, _ := endpoint(t, b, "api", testMesh)
	m := request(t, b, from, "ax.send", object{"target": "api", "text": "question", "client_message_id": "first"}).(Message)
	if err := b.event(m.ID, "content_served", to.epoch); err != nil {
		t.Fatal(err)
	}
	reply := request(t, b, to, "ax.reply", object{"message_id": m.ID, "text": "answer", "client_message_id": "answer"}).(Message)
	if err := b.event(reply.ID, "expired", from.epoch); err != nil {
		t.Fatal(err)
	}
	resent := request(t, b, to, "ax.resend", object{"message_id": reply.ID, "client_message_id": "retry"}).(Message)
	if resent.Parent != m.ID || resent.Depth != reply.Depth || resent.ResendOf != reply.ID {
		t.Fatal("resend lost reply context")
	}
	// A distinct key must not defeat the short-window duplicate guard.
	if _, err := b.request(to, "ax.resend", raw(object{"message_id": reply.ID, "client_message_id": "another"})); err == nil {
		t.Fatal("new key bypassed duplicate suppression")
	}
}
