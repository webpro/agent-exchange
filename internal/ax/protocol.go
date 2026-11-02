package ax

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Content is JSON inside an MCP text string: control-heavy text expands sevenfold,
// while escaped data and provenance can double. Keep room for message metadata.
const maxFrame = 576 << 10
const maxText = 64 << 10
const maxData = 32 << 10
const externalPolicyCode = -32002

type object = map[string]any

type packet struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    object `json:"data,omitempty"`
}

const replyDepthLimitCode = -32001

// A keyed write the broker refused before storing or changing any message; see refuse.
const requestRefusedCode = -32003

func (e *rpcError) Error() string { return e.Message }
func raw(v any) json.RawMessage   { b, _ := json.Marshal(v); return b }
func readFrame(r io.Reader) (packet, error) {
	var n [4]byte
	if _, e := io.ReadFull(r, n[:]); e != nil {
		return packet{}, e
	}
	size := binary.BigEndian.Uint32(n[:])
	if size == 0 || size > maxFrame {
		return packet{}, errors.New("invalid frame size")
	}
	b := make([]byte, int(size))
	if _, e := io.ReadFull(r, b); e != nil {
		return packet{}, e
	}
	var p packet
	if e := json.Unmarshal(b, &p); e != nil {
		return p, e
	}
	if p.JSONRPC != "2.0" {
		return p, errors.New("JSON-RPC 2.0 required")
	}
	return p, nil
}
func writeFrame(w io.Writer, p packet) error {
	p.JSONRPC = "2.0"
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	if len(b) > maxFrame {
		return errors.New("frame too large")
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	for _, buf := range [][]byte{n[:], b} {
		for len(buf) > 0 {
			n, e := w.Write(buf)
			if e != nil {
				return e
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			buf = buf[n:]
		}
	}
	return nil
}

type client struct {
	conn      net.Conn
	mu        sync.Mutex
	writing   chan struct{}
	closed    sync.Once
	next      int
	pending   map[string]chan packet
	offers    chan Message
	notices   chan deliveryNotice
	lifecycle chan struct{}
	peers     chan struct{}
	done      chan struct{}
}

func dial(path string) (*client, error) {
	conn, e := net.DialTimeout("unix", path, time.Second)
	if e != nil {
		return nil, e
	}
	return newClient(conn), nil
}

func newClient(conn net.Conn) *client {
	c := &client{conn: conn, writing: make(chan struct{}, 1), pending: map[string]chan packet{}, offers: make(chan Message, 8), notices: make(chan deliveryNotice, 1), lifecycle: make(chan struct{}, 1), peers: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		defer c.close()
		for {
			p, e := readFrame(conn)
			if e != nil {
				return
			}
			if p.Method == "ax.peers.changed" {
				select {
				case c.peers <- struct{}{}:
				default:
				}
			} else if p.Method == "ax.lifecycle.changed" {
				// State is read from the trusted session file; duplicate hints coalesce.
				select {
				case c.lifecycle <- struct{}{}:
				default:
				}
			} else if p.Method == "ax.delivery.notice" {
				var n deliveryNotice
				if json.Unmarshal(p.Params, &n) != nil {
					return
				}
				select {
				case c.notices <- n:
				default:
					return
				}
			} else if p.Method == "ax.delivery.offer" {
				var m Message
				if json.Unmarshal(p.Params, &m) != nil {
					return
				}
				select {
				case c.offers <- m:
				default:
					return
				}
			} else {
				c.mu.Lock()
				ch := c.pending[string(p.ID)]
				c.mu.Unlock()
				if ch != nil {
					select {
					case ch <- p:
					default:
					}
				}
			}
		}
	}()
	return c
}
func (c *client) close() {
	c.closed.Do(func() { close(c.done); c.conn.Close() })
}

// Once a write starts, a missing response leaves the operation's outcome
// unknown. A retry must retain the original idempotency key.
type transportError struct {
	cause     error
	submitted bool
}

func (e *transportError) Error() string { return e.cause.Error() }
func (e *transportError) Unwrap() error { return e.cause }

func (c *client) call(method string, args any, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.callContext(ctx, method, args, out)
}

func (c *client) callContext(ctx context.Context, method string, args any, out any) error {
	select {
	case c.writing <- struct{}{}:
	case <-ctx.Done():
		return &transportError{cause: ctx.Err()}
	case <-c.done:
		return &transportError{cause: errors.New("broker connection closed")}
	}
	if err := ctx.Err(); err != nil {
		<-c.writing
		return &transportError{cause: err}
	}
	select {
	case <-c.done:
		<-c.writing
		return &transportError{cause: errors.New("broker connection closed")}
	default:
	}
	c.mu.Lock()
	c.next++
	id := fmt.Sprint(c.next)
	ch := make(chan packet, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	stop := context.AfterFunc(ctx, c.close)
	defer stop()
	c.conn.SetWriteDeadline(deadline)
	e := writeFrame(c.conn, packet{ID: json.RawMessage(id), Method: method, Params: raw(args)})
	<-c.writing
	if e != nil {
		c.close()
		return &transportError{cause: e, submitted: true}
	}
	var p packet
	var stopped error
	select {
	case p = <-ch:
	case <-c.done:
		stopped = errors.New("broker connection closed")
	case <-ctx.Done():
		c.close()
		stopped = ctx.Err()
	}
	if stopped != nil {
		// The reader can queue a complete response and then observe EOF before
		// this goroutine runs. Preserve that known outcome over a close/deadline.
		select {
		case p = <-ch:
		default:
			return &transportError{cause: stopped, submitted: true}
		}
	}
	if p.Error != nil {
		return p.Error
	}
	if out != nil {
		if err := json.Unmarshal(p.Result, out); err != nil {
			c.close()
			return &transportError{cause: err, submitted: true}
		}
	}
	return nil
}
