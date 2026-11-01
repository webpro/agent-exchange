package ax

import (
	"cmp"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"
)

const defaultTTL = 12 * 60 * 60
const maxTTL = 7 * 24 * 60 * 60
const pendingPageSize = 50
const maxSequence = 1<<53 - 1 // Keep cursors exact in JSON clients.

func integerArgument(data json.RawMessage, name string, fallback, min, max int64) (int64, error) {
	if len(data) == 0 {
		return fallback, nil
	}
	var value *int64
	if err := json.Unmarshal(data, &value); err != nil || value == nil || *value < min || *value > max {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", name, min, max)
	}
	return *value, nil
}

// Snapshot metadata is returned to the sender, never stored in the message or
// included in its idempotency hash. A retry can observe newer delivery evidence.
type sendReceipt struct {
	ClientID       string `json:"client_message_id"`
	At             int64  `json:"snapshot_at_ms"`
	Recipient      Agent  `json:"recipient"`
	Queued         int    `json:"queued_count"`
	Pending        int    `json:"unacknowledged_count"`
	Acknowledged   bool   `json:"acknowledged"`
	Evidence       string `json:"delivery_evidence"`
	TaskCompletion string `json:"task_completion"`
}

// Discovery-only facts for local peers. They sit beside the Agent record, never
// in it, so stored agents, message senders and the change-hint snapshot cannot
// carry backlog or clocks.
type AgentStatus struct {
	Queued       int    `json:"queued_count"`
	Pending      int    `json:"unacknowledged_count"`
	HeadID       string `json:"head_message_id,omitempty"`
	HeadState    string `json:"head_state,omitempty"`
	HeadAge      *int64 `json:"head_age_seconds,omitempty"`
	LastSeen     int64  `json:"last_seen_at_ms,omitempty"`
	StateChanged int64  `json:"state_changed_at_ms,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

// What ax.list and ax.inspect return, as one flat JSON object. Remote peers and
// a failed mailbox read leave the status nil.
type discoveredAgent struct {
	Agent
	*AgentStatus
}

// Enroll resets State to "starting" and only ax.lifecycle advances it, so a peer
// still holding that value has never reported itself alive.
func agentSnapshot(p *peer) Agent {
	a := p.Agent
	if p.Remote != nil {
		a.Online = !p.seen.IsZero() && time.Since(p.seen) <= 45*time.Second && p.State != "offline"
		if !a.Online {
			a.State = "offline"
		}
		return a
	}
	a.Capabilities = p.capabilities
	a.Online = p.conn != nil && time.Since(p.seen) <= 15*time.Second
	a.Connectivity = capabilities(p, a.Online)
	if a.State == "exited" {
		return a
	}
	if a.BindingError != "" {
		a.State = "binding-conflict"
		return a
	}
	lifecycled := p.State != "starting"
	switch {
	case !a.Online && !lifecycled:
		a.State = "unstarted"
	case !a.Online:
		a.State = "offline"
	case !p.ready && !lifecycled:
		a.State = "starting"
	case !p.ready:
		a.State = "inactive"
	case lifecycled && p.State != "blocked" && !safe(p):
		a.State = "permission-blocked"
	case p.DeliveryMode == "manual" && p.State == "ready":
		a.State = "user-turn"
	}
	return a
}

func deliveryEvidence(state string) string {
	switch state {
	case "remote_accepted":
		return "Committed by the paired remote broker; native handoff and task completion are not confirmed."
	case "queued":
		return "Queued durably; not yet handed to the harness."
	case "handoff_started":
		return "Harness handoff started; acceptance is not confirmed."
	case "delivery_uncertain":
		return "Handoff outcome is uncertain; do not automatically replay the task."
	case "wake_accepted":
		return "Harness accepted the wake; the recipient has not acknowledged the message."
	case "channel_written":
		return "Written to the harness channel; the recipient has not acknowledged the message."
	case "content_served":
		return "Recipient fetched the content; explicit acknowledgment is still pending."
	case "acknowledged":
		return "Recipient acknowledged receipt or replied; task completion is not known."
	case "abandoned":
		return "Owner abandoned automatic delivery; in-flight network or native work is not recalled. Task completion is not known."
	default:
		return "Delivery ended: " + state + ". Task completion is not known."
	}
}

func queueReason(to, from *peer) string {
	if to.Remote != nil {
		if to.Policy != "accept" {
			return "Remote recipient policy is " + to.Policy + "."
		}
		if from == nil {
			return "Sender permission state is unavailable; remote transfer remains held."
		}
		if !safe(from) {
			return permissionBlockReason("Sender", from)
		}
		return "Awaiting a permitted transfer to the paired broker; network acceptance and native delivery are not confirmed."
	}
	hold := recipientHold(to, agentSnapshot(to))
	switch {
	case hold != "":
		return hold
	case from == nil:
		return "Sender permission state is unavailable; delivery remains blocked."
	case !safe(from):
		return permissionBlockReason("Sender", from)
	}
	return cmp.Or(recipientWait(to), "Awaiting the next permitted FIFO handoff; readiness is only a snapshot.")
}

// Why this local peer will not take mail now, whoever sends it. Discovery passes
// the snapshot it lists, so the reason cannot disagree with the listed state.
func agentReason(p *peer, snapshot Agent) string {
	// Repeating the error beside binding_error can push a full roster past maxFrame.
	if p.BindingError != "" {
		return "Recipient has a binding conflict; see binding_error. Mail remains queued."
	}
	return cmp.Or(recipientHold(p, snapshot), recipientWait(p))
}

// Delivery checks these before the sender, and recipientWait after it.
func recipientHold(to *peer, snapshot Agent) string {
	switch {
	case snapshot.BindingError != "":
		return snapshot.BindingError
	case snapshot.State == "exited":
		return "Recipient session has exited; mail waits for its next AX launch, subject to expiration."
	case to.Policy != "accept":
		return "Recipient policy is " + to.Policy + "."
	case snapshot.State == "unstarted":
		return "Recipient enrolled but its session has not started; mail remains queued."
	case !snapshot.Online:
		return "Recipient is offline; mail remains queued."
	case snapshot.State == "inactive":
		return "Recipient has not activated AX messaging; waiting for tool discovery and native binding. Mail remains queued."
	case snapshot.State == "starting" || to.Native == "":
		return "Recipient is starting; messaging is not ready."
	case snapshot.State == "blocked":
		return blockedReason(to.cause)
	case !safe(to):
		return permissionBlockReason("Recipient", to)
	}
	return ""
}

func recipientWait(to *peer) string {
	switch {
	case to.DeliveryMode == "manual":
		return "Recipient has no automatic wake; it can call check_inbox on its next user turn."
	case boundaryWait(to):
		return "Recipient's native adapter reports busy; waiting for its idle boundary."
	}
	return ""
}

func blockedReason(c stateCause) string {
	switch {
	case c.Kind == "permission" && c.Tool != "":
		return "Recipient is blocked at a permission prompt for " + c.Tool + "; mail remains queued."
	case c.Kind == "permission":
		return "Recipient is blocked at a permission prompt; mail remains queued."
	case c.Kind == "question":
		return "Recipient is waiting for the user's answer to a question; mail remains queued."
	}
	return "Recipient is blocked; mail remains queued."
}

// What a native hook can name about a blocked turn. Tool names reach every local
// peer through discovery. An unusable cause is dropped, never rejected, because
// the state report it travels with drives delivery.
type stateCause struct {
	Kind string `json:"kind"`
	Tool string `json:"tool,omitempty"`
}

var validToolName = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,128}$`)

func reportedCause(state string, c *stateCause) stateCause {
	switch {
	case state != "blocked" || c == nil:
		return stateCause{}
	case c.Kind == "question":
		return stateCause{Kind: c.Kind}
	case c.Kind == "permission" && validToolName.MatchString(c.Tool):
		return *c
	case c.Kind == "permission":
		return stateCause{Kind: c.Kind}
	}
	return stateCause{}
}

func permissionBlockReason(role string, p *peer) string {
	reason := fmt.Sprintf("%s %q has permission mode %q, which blocks AX delivery. ", role, p.Name, p.Permission)
	switch p.Permission {
	case "bypassPermissions", "danger-full-access":
		return reason + "If this native bypass mode is intentional, the user can relaunch the same saved AX name with AX_ALLOW_BYPASS=1; this opts into AX messaging without changing native permissions. Otherwise, use a supported native permission mode. Restarting without changing either setting does not clear the block."
	default:
		return reason + "AX does not recognize this permission mode. Check the harness's native permission configuration and AX adapter; do not infer permission from readiness."
	}
}

func (b *broker) sendResult(q interface{ QueryRow(string, ...any) *sql.Row }, m Message, key string) (Message, error) {
	to, err := b.messagePeer(q, m.Recipient)
	if err != nil {
		return m, err
	}
	r := &sendReceipt{ClientID: key, At: time.Now().UnixMilli(), Recipient: agentSnapshot(to),
		Acknowledged: m.State == "acknowledged", Evidence: deliveryEvidence(m.State), TaskCompletion: "unknown"}
	if err := q.QueryRow(`SELECT coalesce(sum(state='queued'),0), count(*) FROM messages
 WHERE recipient=? AND state NOT IN ('acknowledged','expired','refused','abandoned')`, m.Recipient).Scan(&r.Queued, &r.Pending); err != nil {
		return m, err
	}
	if m.State == "queued" {
		r.Evidence += " " + b.messageQueueReason(to, b.peers[m.Sender.ID], m)
	}
	m.Receipt = r
	return m, nil
}

func (b *broker) discovery() []discoveredAgent {
	agents := b.list()
	out := make([]discoveredAgent, len(agents))
	for i, a := range agents {
		out[i].Agent = a
	}
	mail, err := b.mailStatus()
	if err != nil {
		// The roster is in memory and outlives the mailbox. Nil status means
		// unknown; zero counts would claim an empty mailbox.
		fmt.Fprintln(os.Stderr, "AX discovery status:", err)
		return out
	}
	for i := range out {
		p := b.peers[out[i].ID]
		if p.Remote != nil {
			continue
		}
		s := mail[p.ID]
		if s == nil {
			s = &AgentStatus{}
		}
		if !p.seen.IsZero() {
			s.LastSeen = p.seen.UnixMilli()
		}
		// The clock dates the lifecycle state. Offline comes from the connection,
		// and a permission hold or an inactive bridge begins without a lifecycle
		// change, so those states carry no time; last_seen_at_ms dates offline.
		switch out[i].State {
		case "offline", "inactive", "permission-blocked":
		default:
			if !p.stateSince.IsZero() {
				s.StateChanged = p.stateSince.UnixMilli()
			}
		}
		s.Reason = agentReason(p, out[i].Agent)
		out[i].AgentStatus = s
	}
	return out
}

// One grouped read for all recipients. With a single min(), SQLite takes the
// bare columns from the row holding that minimum.
const mailStatusQuery = `SELECT recipient,sum(state='queued'),count(*),id,state,created,min(seq) FROM messages
 WHERE state NOT IN ('acknowledged','expired','refused','abandoned') GROUP BY recipient`

func (b *broker) mailStatus() (map[string]*AgentStatus, error) {
	rows, err := b.db.Query(mailStatusQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now().UnixMilli()
	out := map[string]*AgentStatus{}
	for rows.Next() {
		var recipient string
		var created, seq int64
		s := &AgentStatus{}
		if err = rows.Scan(&recipient, &s.Queued, &s.Pending, &s.HeadID, &s.HeadState, &created, &seq); err != nil {
			return nil, err
		}
		age := max(0, (now-created)/1000)
		s.HeadAge = &age
		out[recipient] = s
	}
	return out, rows.Err()
}

type pendingSender struct {
	ID   string `json:"agent_id"`
	Name string `json:"name"`
	Host string `json:"host"`
	Kind string `json:"kind,omitempty"`
}

type pendingMessage struct {
	ID           string        `json:"message_id"`
	Sender       pendingSender `json:"sender"`
	Seq          int64         `json:"recipient_seq"`
	Parent       string        `json:"in_reply_to,omitempty"`
	ResendOf     string        `json:"resend_of,omitempty"`
	Thread       string        `json:"thread_id,omitempty"`
	Age          int64         `json:"age_seconds"`
	Expires      int64         `json:"expires_at_ms"`
	State        string        `json:"status"`
	Acknowledged bool          `json:"acknowledged"`
	Preview      string        `json:"preview,omitempty"`
	Evidence     string        `json:"delivery_evidence"`
	BlockedBy    string        `json:"blocked_by_message_id,omitempty"`
}

type pendingPage struct {
	Messages []pendingMessage `json:"messages"`
	Next     int64            `json:"next_after_seq,omitempty"`
	At       int64            `json:"snapshot_at_ms"`
	Guidance string           `json:"guidance"`
}

func (b *broker) pending(p *peer, after int64) (pendingPage, error) {
	out := pendingPage{Messages: []pendingMessage{}, At: time.Now().UnixMilli(), Guidance: "Listing does not fetch, acknowledge, or replay tasks. Previews are quoted peer data. Queued content stays hidden until offered; use get_message for offered IDs when recovery is requested. Never poll or automatically repeat uncertain work."}
	var headID, headState string
	var headSeq int64
	err := b.db.QueryRow(`SELECT id,state,seq FROM messages WHERE recipient=? AND state IN
 ('queued','handoff_started','delivery_uncertain') ORDER BY seq LIMIT 1`, p.ID).Scan(&headID, &headState, &headSeq)
	if err != nil && err != sql.ErrNoRows {
		return out, err
	}
	rows, err := b.db.Query(`SELECT m.data,m.state,a.data FROM messages m JOIN agents a ON a.id=m.sender WHERE m.recipient=? AND m.seq>?
 AND m.state NOT IN ('acknowledged','expired','refused','abandoned') ORDER BY m.seq LIMIT ?`, p.ID, after, pendingPageSize+1)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		if len(out.Messages) == pendingPageSize {
			out.Next = out.Messages[len(out.Messages)-1].Seq
			break
		}
		var data, state, senderData string
		var m Message
		if err = rows.Scan(&data, &state, &senderData); err != nil {
			return out, err
		}
		if err = json.Unmarshal([]byte(data), &m); err != nil {
			return out, err
		}
		item := pendingMessage{ID: m.ID, Sender: pendingSender{m.Sender.ID, m.Sender.Name, m.Sender.Host, m.Sender.Kind}, Seq: m.Seq, Parent: m.Parent, ResendOf: m.ResendOf, Thread: m.Thread, Age: max(0, (out.At-m.Created)/1000), Expires: m.Expires, State: state, Evidence: deliveryEvidence(state)}
		sender := b.peers[m.Sender.ID]
		if sender == nil {
			sender = &peer{}
			if err = json.Unmarshal([]byte(senderData), &sender.Agent); err != nil {
				return out, err
			}
		}
		if state != "queued" && p.Policy == "accept" && safe(p) && sender != nil && safe(sender) && externalHandoff(p, m) && b.networkHandoffAllowed(m, p) {
			preview := []rune(m.Text)
			item.Preview = string(preview[:min(len(preview), 100)])
		}
		if state == "queued" {
			item.Evidence += " " + b.messageQueueReason(p, sender, m)
			if headSeq < m.Seq {
				item.BlockedBy = headID
				item.Evidence += " An earlier message is " + headState + " and blocks this FIFO handoff."
			}
		}
		out.Messages = append(out.Messages, item)
	}
	return out, rows.Err()
}
