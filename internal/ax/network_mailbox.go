//go:build darwin || linux

package ax

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

var errNetworkCacheFull = errors.New("remote identity cache is full; local endpoint slots are reserved")

// These are private broker operations for the network adapter, never TCP RPCs
// or model tools. The A2A binding will translate its authenticated requests here.
type networkRoute struct {
	Alias       string `json:"alias"`
	Fingerprint string `json:"fingerprint"`
	AgentID     string `json:"agent_id"`
}
type networkOrigin struct {
	Fingerprint string `json:"fingerprint"`
	MessageID   string `json:"message_id"`
}
type networkEnvelope struct {
	ID             string          `json:"message_id"`
	SenderID       string          `json:"sender_id"`
	SenderName     string          `json:"sender_name"`
	SenderHost     string          `json:"sender_host"`
	Recipient      string          `json:"recipient_id"`
	Text           string          `json:"text"`
	Parent         string          `json:"in_reply_to,omitempty"`
	ParentSourceID string          `json:"source_parent_id,omitempty"`
	Expires        int64           `json:"expires_at_ms"`
	Provenance     json.RawMessage `json:"provenance,omitempty"`
	Data           json.RawMessage `json:"data,omitempty"`
}
type networkImport struct {
	Expires        int64
	Hash           string
	Origin         networkOrigin
	ParentRemoteID string
}

func (b *broker) networkPeer(alias string) (networkPeer, error) {
	peers, err := networkPeers(b.dir)
	if err != nil {
		return networkPeer{}, err
	}
	peer, exists := peers[alias]
	if !exists || !peer.Messaging {
		return peer, errors.New("paired peer has no AX messaging grant")
	}
	_, err = validateNetworkCard(peer.Card)
	return peer, err
}

func (b *broker) networkRouteAllowed(route *networkRoute) bool {
	if route == nil {
		return false
	}
	peer, err := b.networkPeer(route.Alias)
	return err == nil && networkFingerprint(peer.Card) == route.Fingerprint
}

// Discovery can include many agents on one installation. Read policy and verify
// each installation once per list, rather than once per displayed agent.
func (b *broker) networkAllowedRoutes() map[string]string {
	allowed := map[string]string{}
	peers, err := networkPeers(b.dir)
	if err != nil {
		return allowed
	}
	for alias, paired := range peers {
		if paired.Messaging {
			if _, err := validateNetworkCard(paired.Card); err == nil {
				allowed[alias] = networkFingerprint(paired.Card)
			}
		}
	}
	return allowed
}

func (b *broker) networkProxy(alias string, paired networkPeer, id, name, host string) (*peer, error) {
	if !validID.MatchString(id) || !validName.MatchString(name) || harnesses[host].nativeID == nil {
		return nil, errors.New("invalid remote agent identity")
	}
	fingerprint := networkFingerprint(paired.Card)
	localID := "net_" + digest(fingerprint + ":" + id)[:40]
	if b.peers[localID] == nil {
		remoteCount := 0
		for _, current := range b.peers {
			if current.Remote != nil {
				remoteCount++
			}
		}
		// Reserve local slots even when an authorized peer sends many identities.
		if remoteCount >= 64 || len(b.peers) >= 192 {
			return nil, errNetworkCacheFull
		}
	}
	p, err := b.messagePeer(b.db, localID)
	if errors.Is(err, sql.ErrNoRows) {
		// This proxy is a transport conduit. The destination's actual external
		// policy is enforced again by networkReceive before native enqueueing.
		p = &peer{Agent: Agent{ID: localID, Kind: "remote", Permission: "network", Policy: "accept", External: "all"}, hash: digest(randomID(""))}
	} else if err != nil {
		return nil, err
	}
	if p.Remote == nil && p.Kind != "remote" {
		return nil, errors.New("remote identity conflicts with a local endpoint")
	}
	p.Name, p.Host = name+"@"+alias, host
	if p.State == "" || p.State == "exited" {
		p.State = "offline"
	} // Receiving mail does not prove native readiness.
	p.Remote = &networkRoute{Alias: alias, Fingerprint: fingerprint, AgentID: id}
	p.seen = time.Now()
	if err := b.save(p); err != nil {
		return nil, err
	}
	b.peers[p.ID] = p
	return p, nil
}

func (b *broker) networkReceive(alias string, envelope networkEnvelope) (Message, error) {
	paired, err := b.networkPeer(alias)
	if err != nil {
		return Message{}, err
	}
	if !validID.MatchString(envelope.ID) || envelope.Expires <= 0 || envelope.Expires > time.Now().Add(maxTTL*time.Second).UnixMilli() {
		return Message{}, errors.New("invalid remote message ID or expiration")
	}
	to, err := b.messagePeer(b.db, envelope.Recipient)
	if err != nil || to.Remote != nil || !(slices.Contains(paired.Exports, "*") || slices.Contains(paired.Exports, to.Name)) {
		return Message{}, errors.New("recipient is outside the peer's messaging scope")
	}
	from, err := b.networkProxy(alias, paired, envelope.SenderID, envelope.SenderName, envelope.SenderHost)
	if err != nil {
		return Message{}, err
	}
	method := "ax.send"
	if envelope.Parent != "" {
		if !validID.MatchString(envelope.ParentSourceID) {
			return Message{}, errors.New("remote parent must include its source message ID")
		}
		parent, err := b.message(envelope.Parent)
		if err != nil {
			return Message{}, err
		}
		switch {
		case parent.Recipient == from.ID && parent.Sender.ID == to.ID:
			method = "ax.reply"
		case parent.Sender.ID == from.ID && parent.Recipient == to.ID:
			method = "ax.follow_up"
		default:
			return Message{}, errors.New("remote parent belongs to another conversation")
		}
	}
	options := &networkImport{Expires: envelope.Expires, Hash: digest(string(raw(envelope))), Origin: networkOrigin{Fingerprint: networkFingerprint(paired.Card), MessageID: envelope.ID}, ParentRemoteID: envelope.ParentSourceID}
	result, err := b.sendImported(from, to.ID, envelope.Parent, envelope.Text, envelope.ID, defaultTTL, method, "", options, envelope.Provenance, envelope.Data)
	if err != nil {
		return Message{}, err
	}
	return result.(Message), nil
}

func (b *broker) networkOutbox(alias string) (*networkEnvelope, error) {
	return b.networkOutboxExcept(alias, nil)
}

func (b *broker) networkOutboxExcept(alias string, skip []string) (*networkEnvelope, error) {
	paired, err := b.networkPeer(alias)
	if err != nil {
		return nil, err
	}
	// One FIFO head per remote recipient. A held sender must not starve other
	// recipients, but later mail to that same recipient cannot overtake it.
	rows, err := b.db.Query(`SELECT m.id FROM messages m JOIN agents a ON a.id=m.recipient
 WHERE m.state='queued' AND json_extract(a.data,'$.remote.alias')=?
 AND json_extract(a.data,'$.remote.fingerprint')=?
 AND NOT EXISTS (SELECT 1 FROM messages prior WHERE prior.recipient=m.recipient
 AND prior.seq<m.seq AND prior.state IN ('queued','handoff_started','delivery_uncertain'))
 ORDER BY m.created,m.seq LIMIT 256`, alias, networkFingerprint(paired.Card))
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		m, err := b.message(id)
		if err != nil {
			return nil, err
		}
		from, err := b.messagePeer(b.db, m.Sender.ID)
		if err != nil || from.Remote != nil || !safe(from) || m.Expires <= time.Now().UnixMilli() {
			continue
		}
		to, err := b.messagePeer(b.db, m.Recipient)
		if err != nil {
			return nil, err
		}
		if slices.Contains(skip, to.Remote.AgentID) || to.Policy != "accept" {
			continue
		}
		parentID := ""
		if m.Parent != "" {
			parent, err := b.message(m.Parent)
			if err != nil {
				return nil, err
			}
			if parent.NetworkOrigin != nil && parent.NetworkOrigin.Fingerprint == to.Remote.Fingerprint {
				parentID = parent.NetworkOrigin.MessageID
			} else {
				parentID = parent.RemoteMessageID
			}
			if parentID == "" {
				continue // Its parent's destination commit is not known yet.
			}
		}
		return &networkEnvelope{ID: m.ID, SenderID: from.ID, SenderName: from.Name, SenderHost: from.Host, Recipient: to.Remote.AgentID, Text: m.Text, Parent: parentID, ParentSourceID: m.Parent, Expires: m.Expires, Provenance: m.Provenance, Data: m.Data}, nil
	}
	return nil, nil
}

func (b *broker) networkAccepted(alias, id, remoteID string) (Message, error) {
	if !validID.MatchString(remoteID) {
		return Message{}, errors.New("invalid remote message receipt")
	}
	m, err := b.message(id)
	if err != nil {
		return Message{}, err
	}
	to, err := b.messagePeer(b.db, m.Recipient)
	if err != nil || to.Remote == nil || to.Remote.Alias != alias || !b.networkRouteAllowed(to.Remote) {
		return Message{}, errors.New("receipt does not belong to a permitted remote route")
	}
	if m.RemoteMessageID != "" {
		if m.RemoteMessageID != remoteID {
			return Message{}, errors.New("remote receipt conflicts with the original acceptance")
		}
		return m, nil
	}
	m.RemoteMessageID = remoteID
	if m.State == "queued" {
		m.State = "remote_accepted"
	}
	tx, err := b.db.Begin()
	if err != nil {
		return Message{}, err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE messages SET data=?,state=? WHERE id=?", string(raw(m)), m.State, m.ID); err != nil {
		return Message{}, err
	}
	if _, err = tx.Exec("INSERT INTO events(message,state,at,epoch) VALUES(?,?,?,0)", m.ID, "remote_accepted", time.Now().UnixMilli()); err != nil {
		return Message{}, err
	}
	return m, tx.Commit()
}

func (b *broker) networkHandoffAllowed(m Message, to *peer) bool {
	if m.NetworkOrigin == nil {
		return true
	}
	peers, err := networkPeers(b.dir)
	if err != nil {
		return false
	}
	for alias, paired := range peers {
		if networkFingerprint(paired.Card) == m.NetworkOrigin.Fingerprint {
			_, err := b.networkPeer(alias)
			return err == nil && (slices.Contains(paired.Exports, "*") || slices.Contains(paired.Exports, to.Name))
		}
	}
	return false
}

func (b *broker) networkRequest(method string, params json.RawMessage) (any, error) {
	var a struct {
		Skip        []string        `json:"skip_recipients"`
		Fingerprint string          `json:"fingerprint"`
		Alias       string          `json:"alias"`
		ID          string          `json:"message_id"`
		Recipient   string          `json:"recipient_id"`
		After       string          `json:"after"`
		RemoteID    string          `json:"remote_message_id"`
		Agents      []networkAgent  `json:"agents"`
		Envelope    networkEnvelope `json:"envelope"`
	}
	if json.Unmarshal(params, &a) != nil || !validOrigin.MatchString(a.Alias) {
		return nil, errors.New("invalid network adapter request")
	}
	if a.Fingerprint != "" {
		paired, err := b.networkPeer(a.Alias)
		if err != nil || networkFingerprint(paired.Card) != a.Fingerprint {
			return nil, errors.New("paired identity changed during network request")
		}
	}
	switch method {
	case "ax.network.task":
		return b.networkTask(a.Alias, a.Recipient, a.ID)
	case "ax.network.tasks":
		return b.networkTaskList(a.Alias, a.Recipient, a.After)
	case "ax.network.cancel":
		m, err := b.networkTask(a.Alias, a.Recipient, a.ID)
		if err != nil {
			return nil, err
		}
		if m.TaskOutcome == "canceled" {
			return m, nil
		}
		if m.State != "queued" {
			return nil, errors.New("task cannot be canceled after native handoff")
		}
		return b.saveNetworkTask(m, "canceled", true)
	case "ax.network.receive":
		return b.networkReceive(a.Alias, a.Envelope)
	case "ax.network.outbox":
		if len(a.Skip) > 64 {
			return nil, errors.New("too many skipped recipients")
		}
		return b.networkOutboxExcept(a.Alias, a.Skip)
	case "ax.network.accepted":
		return b.networkAccepted(a.Alias, a.ID, a.RemoteID)
	case "ax.network.sync":
		paired, err := b.networkPeer(a.Alias)
		if err != nil {
			return nil, err
		}
		if len(a.Agents) > networkDiscoveryLimit {
			return nil, errors.New("too many remote agents")
		}
		skipped := 0
		for _, agent := range a.Agents {
			p, err := b.networkProxy(a.Alias, paired, agent.ID, agent.Name, agent.Host)
			if errors.Is(err, errNetworkCacheFull) {
				skipped++
				continue
			}
			if err != nil {
				return nil, err
			}
			p.State = agent.State
			if err = b.save(p); err != nil {
				return nil, err
			}
		}
		return object{"ok": true, "synced": len(a.Agents) - skipped, "skipped_cache_full": skipped}, nil
	default:
		return nil, fmt.Errorf("unsupported private network operation %q", method)
	}
}

func (b *broker) reapNetworkPeers(now time.Time) {
	var routes map[string]string
	for id, p := range b.peers {
		if p.Remote == nil {
			continue
		}
		if routes == nil {
			routes = b.networkAllowedRoutes()
		}
		lastSeen := p.seen
		if lastSeen.IsZero() {
			lastSeen = p.enrolled
		}
		if routes[p.Remote.Alias] == p.Remote.Fingerprint && now.Sub(lastSeen) < time.Minute {
			continue
		}
		// Preserve mail, identities, and idempotency keys in SQLite for later resume.
		p.State = "exited"
		if b.save(p) == nil {
			delete(b.peers, id)
		}
	}
}
