package ax

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func (b *broker) messagePeer(q interface{ QueryRow(string, ...any) *sql.Row }, id string) (*peer, error) {
	if p := b.peers[id]; p != nil {
		return p, nil
	}
	return storedPeer(q, id)
}

// Archived identities stay in SQLite for resume, mail, and idempotency, but do
// not occupy an active endpoint slot or participate in the dispatch sweep.
func storedPeer(q interface{ QueryRow(string, ...any) *sql.Row }, id string) (*peer, error) {
	p := &peer{}
	var data string
	if err := q.QueryRow("SELECT hash,data,epoch FROM agents WHERE id=?", id).Scan(&p.hash, &data, &p.epoch); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(data), &p.Agent); err != nil {
		return nil, err
	}
	p.Online = false
	return p, nil
}

func (b *broker) savedTarget(target string) (*peer, error) {
	rows, err := b.db.Query("SELECT id FROM agents WHERE id=? OR json_extract(data,'$.name')=? LIMIT 2", target, target)
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
	if len(ids) == 0 {
		return nil, &targetError{"unknown_target", fmt.Sprintf("agent %q not found in this AX runtime; launch it with ax, or check whether it is running under a different AX_HOME", target)}
	}
	if len(ids) != 1 {
		return nil, &targetError{"ambiguous_target", "ambiguous agent name; list agents and use agent_id"}
	}
	return storedPeer(b.db, ids[0])
}

// The native session's lock, not a bridge heartbeat, proves its lifetime. Use
// the broker's existing timer with a bounded scan; never add a per-agent poller.
func (b *broker) reapSessions(now time.Time) {
	if now.Sub(b.lastReap) < 5*time.Second {
		return
	}
	b.lastReap = now
	b.reapNetworkPeers(now)
	for _, p := range b.peers {
		if p.Remote != nil {
			continue
		}
		b.reapSession(p, now)
	}
}

func (b *broker) reapSession(p *peer, now time.Time) {
	path, err := namedSessionPath(filepath.Join(b.dir, "sessions"), p.Name)
	if err != nil {
		return // Ambiguous or unreadable state is not proof of exit.
	}
	lock, err := privateFile(path+".lock", syscall.O_RDWR)
	if err == nil {
		defer lock.Close()
		if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
			return
		}
	} else if !os.IsNotExist(err) {
		return
	} else if p.conn != nil && now.Sub(p.seen) <= 15*time.Second {
		return // Legacy or externally enrolled bridges may have no launcher lock.
	}
	// A pane reservation or legacy bridge gets a short startup/reconnect window.
	if (p.State == "starting" || os.IsNotExist(err)) && now.Sub(p.enrolled) < 30*time.Second {
		return
	}
	before := p.Agent
	p.State, p.Online = "exited", false
	if err = b.save(p); err != nil {
		p.Agent = before
		return
	}
	if p.conn != nil {
		p.conn.Close()
	}
	b.uncertain(p.ID, p.epoch)
	delete(b.peers, p.ID)
	b.peersChanged()
}

// Exited recipients are absent from dispatch. Expire their queued work in a
// bounded maintenance batch, retaining the usual receipt and failure notice.
func (b *broker) expireArchived(now time.Time) {
	rows, err := b.db.Query(`SELECT m.id FROM messages m JOIN agents a ON a.id=m.recipient
 WHERE m.state='queued' AND m.expires<=? AND json_extract(a.data,'$.state')='exited'
 ORDER BY m.expires LIMIT 256`, now.UnixMilli())
	if err != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return
	}
	for _, id := range ids {
		if b.event(id, "expired", 0) != nil {
			return
		}
	}
}
