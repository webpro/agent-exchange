package ax

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise the real MCP discovery and broker lifecycle path, not bootstrap calls.
func TestReadinessEventsDeliverBeforeHeartbeat(t *testing.T) {
	for _, host := range []string{"claude", "codex"} {
		for _, discoveryFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/discoveryFirst=%v", host, discoveryFirst), func(t *testing.T) {
				dir := startTestServer(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s := Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: "api", Host: host, Mesh: testMesh, Native: uuid()}
				file := filepath.Join(dir, "session.json")
				nativeWakes := make(chan string, 4)
				var err error
				var stopAdapter func()
				s.AdapterSocket, stopAdapter, err = startAdapterHost(ctx, dir, file, nil, func(_ context.Context, native, text string) error {
					if native != s.Native {
						return fmt.Errorf("wrong native session: %s", native)
					}
					nativeWakes <- text
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				defer stopAdapter()
				if err = saveSession(file, s); err != nil {
					t.Fatal(err)
				}
				admin, err := dial(socketPath(dir))
				if err != nil {
					t.Fatal(err)
				}
				defer admin.close()
				if err = admin.call("ax.enroll", s, nil); err != nil {
					t.Fatal(err)
				}
				sender := connectDiscoveryPeer(t, dir, Session{ID: randomID("agt_"), Secret: randomID(""), Name: "web", Host: "codex", Mesh: testMesh, Native: uuid()})
				var sent Message
				if err = sender.call("ax.send", object{"target": "api", "text": "queued before startup", "client_message_id": "first"}, &sent); err != nil {
					t.Fatal(err)
				}
				hook := func() {
					t.Helper()
					if err := Hook(dir, file, bytes.NewReader(raw(object{"session_id": s.Native, "hook_event_name": "SessionStart", "permission_mode": "default"}))); err != nil {
						t.Fatal(err)
					}
				}
				if !discoveryFirst {
					hook()
				}
				in, input := io.Pipe()
				output, out := io.Pipe()
				frames := make(chan packet, 16)
				go func() {
					defer close(frames)
					dec := json.NewDecoder(output)
					for {
						var p packet
						if dec.Decode(&p) != nil {
							return
						}
						frames <- p
					}
				}()
				done := make(chan error, 1)
				go func() { done <- Bridge(ctx, dir, file, in, out) }()
				defer func() { input.Close(); cancel(); <-done; out.Close(); output.Close() }()
				enc := json.NewEncoder(input)
				discover := func() {
					t.Helper()
					// A pipe write blocks forever once the bridge has exited without reading it.
					wrote := make(chan error, 1)
					go func() { wrote <- enc.Encode(packet{JSONRPC: "2.0", ID: raw(1), Method: "tools/list"}) }()
					select {
					case err := <-wrote:
						if err != nil {
							t.Fatal(err)
						}
					case err := <-done:
						done <- err
						t.Fatalf("bridge exited before discovery: %v", err)
					case <-time.After(time.Second):
						t.Fatal("MCP discovery request was not read")
					}
					select {
					case p := <-frames:
						if len(p.Result) == 0 {
							t.Fatalf("unexpected frame: %+v", p)
						}
					case <-time.After(time.Second):
						t.Fatal("no MCP discovery response")
					}
				}
				discover()
				if discoveryFirst {
					hook()
				}
				select {
				case p := <-frames:
					if host != "claude" || p.Method != "notifications/claude/channel" || !bytes.Contains(p.Params, []byte(sent.ID)) {
						t.Fatalf("unexpected delivery: %+v", p)
					}
				case text := <-nativeWakes:
					if host != "codex" || !strings.Contains(text, sent.ID) {
						t.Fatalf("unexpected native wake: %s", text)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("delivery waited for the five-second heartbeat")
				}
				// Neither repeated discovery nor a repeated native hook creates setup turns.
				discover()
				hook()
				if len(frames) != 0 || len(nativeWakes) != 0 {
					t.Fatal("repeated readiness generated a wake")
				}
			})
		}
	}
}

func TestLifecycleHintsAreOptInAndCoalesce(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			b, _ := localBroker(t)
			s, previous, _ := endpoint(t, b, "api", testMesh)
			b.disconnect(previous)
			x, y := net.Pipe()
			defer x.Close()
			c := newClient(y)
			defer c.close()
			current := &serverConn{Conn: x}
			request(t, b, current, "ax.connect", object{"version": "1", "agent_id": s.ID, "secret": s.Secret, "lifecycle_events": enabled})
			args := object{"agent_id": s.ID, "secret": "wrong", "native_session_id": s.Native, "state": "busy"}
			if _, err := b.request(&serverConn{}, "ax.lifecycle", raw(args)); err == nil {
				t.Fatal("accepted untrusted lifecycle")
			}
			if len(c.lifecycle) != 0 {
				t.Fatal("untrusted hook generated a hint")
			}
			args["secret"] = s.Secret
			// Leave hints unread: a burst must not block the socket reader.
			for i := range 100 {
				args["state"] = []string{"busy", "ready"}[i%2]
				request(t, b, &serverConn{}, "ax.lifecycle", args)
			}
			if enabled {
				select {
				case <-c.lifecycle:
				case <-time.After(time.Second):
					t.Fatal("missing lifecycle hint")
				}
			} else {
				select {
				case <-c.lifecycle:
					t.Fatal("legacy client received an unsolicited hint")
				case <-time.After(20 * time.Millisecond):
				}
			}
		})
	}
}
