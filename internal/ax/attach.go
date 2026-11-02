package ax

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Capabilities describe the transport contract, not proof that an agent acted.
// Only an actual reply can complete ax verify.
type agentCapabilities struct {
	Tools        bool   `json:"tools_ready"`
	Wake         string `json:"wake"`
	Confirmation string `json:"confirmation"`
}

func capabilities(p *peer, online bool) agentCapabilities {
	c := agentCapabilities{Tools: online && p.ready, Wake: "native", Confirmation: "host_acceptance"}
	switch {
	case p.DeliveryMode == "mcp":
		c.Wake, c.Confirmation = "mcp", "channel_write"
	case p.DeliveryMode == "manual":
		c.Wake, c.Confirmation = "user_turn", "content_fetch"
	case p.Host == "claude" || p.Host == "pi":
		c.Wake, c.Confirmation = "channel", "channel_write"
	case p.DeliveryMode == "adapter":
		c.Wake = "adapter"
	}
	return c
}

func attach(ctx context.Context, dir string, args []string, in io.Reader, out io.Writer, hardStop func()) error {
	f := flag.NewFlagSet("attach", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	name := f.String("n", "", "AX name")
	native := f.String("s", "", "stable native conversation ID")
	permission := f.String("p", "unknown", "native permission mode")
	relay := f.Bool("relay", false, "external relay endpoint")
	notify := f.Bool("notify", false, "receive MCP notifications")
	external := f.String("external", os.Getenv("AX_EXTERNAL"), "external policy: refuse, verified, all")
	wake := f.String("w", "", "private native wake socket")
	if err := f.Parse(args); err != nil {
		return err
	}
	if !validExternal(*external) {
		return errors.New("external must be refuse, verified, or all")
	}
	kind := ""
	if *relay {
		invalid := false
		f.Visit(func(flag *flag.Flag) {
			if flag.Name == "s" || flag.Name == "p" || flag.Name == "w" {
				invalid = true
			}
		})
		if invalid {
			return errors.New("usage: ax attach --relay -n NAME [--external POLICY]")
		}
		kind, *native, *permission, *notify = "relay", "relay-"+*name, "relay", true
	}
	if *notify && *wake != "" {
		return errors.New("--notify cannot be combined with -w")
	}
	if f.NArg() != 0 || !validName.MatchString(*name) || !validID.MatchString(*native) {
		return errors.New("usage: ax attach -n NAME -s SESSION_ID [-p PERMISSION] [-w PRIVATE_SOCKET]")
	}
	mode := "manual"
	if *notify {
		mode = "mcp"
	}
	if *wake != "" {
		// The local adapter belongs to the same OS user. Never accept HTTP URLs
		// or public listeners through this attachment path.
		info, err := os.Lstat(*wake)
		if err != nil {
			return fmt.Errorf("wake socket: %w", err)
		}
		if !filepath.IsAbs(*wake) || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
			return errors.New("wake socket must be an absolute, private Unix socket owned by you")
		}
		mode = "adapter"
	}
	s, file, release, err := attachedSessionOptions(dir, *name, *native, mode, *wake, kind, *external)
	if err != nil {
		return err
	}
	defer release()
	if err = bindAdapter(dir, file, s.Native, *permission, "ready"); err != nil {
		return err
	}
	// The runtime owns this MCP child; closing it must never signal the host.
	done := make(chan struct{})
	defer close(done)
	if input, ok := in.(io.ReadCloser); ok {
		go func() {
			select {
			case <-ctx.Done():
				input.Close()
			case <-done:
			}
		}()
	}
	return runBridge(ctx, dir, file, in, out, hardStop)
}

func attachedSession(dir, name, native, mode, socket string) (Session, string, func(), error) {
	return attachedSessionOptions(dir, name, native, mode, socket, "", "")
}

func attachedSessionOptions(dir, name, native, mode, socket, kind, external string) (Session, string, func(), error) {
	var s Session
	noop := func() {}
	if err := ensureBroker(dir); err != nil {
		return s, "", noop, err
	}
	c, err := dial(socketPath(dir))
	if err != nil {
		return s, "", noop, err
	}
	defer c.close()
	var features struct {
		LocalAttach bool `json:"local_attach"`
		Relay       bool `json:"relay"`
	}
	if err = c.call("ax.ping", object{}, &features); err != nil {
		return s, "", noop, err
	}
	if !features.LocalAttach {
		return s, "", noop, errors.New("running broker does not support local attachment; finish the broker update before attaching")
	}
	if (kind == "relay" || mode == "mcp" || external != "" && external != "refuse") && !features.Relay {
		return s, "", noop, errors.New("running broker does not support relays; finish the broker update before attaching")
	}
	sessions := filepath.Join(dir, "sessions")
	if err = privateDir(sessions); err != nil {
		return s, "", noop, err
	}
	file, err := namedSessionPath(sessions, name)
	if err != nil {
		return s, "", noop, err
	}
	lock, err := lockFile(file + ".lock")
	if err != nil {
		return s, "", noop, fmt.Errorf("agent %q is already running", name)
	}
	release := func() { lock.Close() }
	ok := false
	defer func() {
		if !ok {
			release()
		}
	}()
	s, err = loadSession(file)
	if os.IsNotExist(err) {
		s = Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: name, Host: "external", Native: native, Kind: kind}
	} else if err != nil {
		return s, "", noop, err
	}
	if s.Host != "external" || s.Native != native || s.Kind != kind || s.SpawnToken != "" {
		return s, "", noop, errors.New("AX name belongs to another runtime or conversation; choose a new name")
	}
	s.Workspace, err = os.Getwd()
	if err != nil {
		return s, "", noop, err
	}
	s.Mesh, err = meshFor(s.Workspace)
	if err != nil {
		return s, "", noop, err
	}
	s.DeliveryMode, s.AdapterSocket = mode, socket
	s.External = external
	s.Started, s.BindingError = true, ""
	s.AllowBypass = bypassOptIn(s.AllowBypass, false)
	if err = saveSession(file, s); err != nil {
		return s, "", noop, err
	}
	if err = c.call("ax.enroll", s, nil); err != nil {
		return s, "", noop, err
	}
	ok = true
	return s, file, release, nil
}

// A manual endpoint receives only when its host gives it a turn. This is a
// separate operation so list_pending remains a read-only recovery tool.
func (b *broker) checkInbox(p *peer) (any, error) {
	if p.DeliveryMode != "manual" {
		return nil, errors.New("check_inbox is only for endpoints without automatic wake")
	}
	if !p.ready || p.State == "starting" || p.State == "blocked" || p.Native == "" || p.Policy != "accept" || !safe(p) {
		return nil, errors.New("AX delivery is blocked by readiness or permission policy")
	}
	var id, state string
	var expires int64
	err := b.db.QueryRow("SELECT id,state,expires FROM messages WHERE recipient=? AND state IN ('queued','handoff_started','delivery_uncertain') ORDER BY seq LIMIT 1", p.ID).Scan(&id, &state, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return object{"message": nil}, nil
	}
	if err != nil {
		return nil, err
	}
	if state != "queued" {
		return nil, fmt.Errorf("message %s has uncertain delivery; recover it explicitly with get_message before receiving more mail", id)
	}
	if expires <= time.Now().UnixMilli() {
		return nil, errors.New("the oldest message expired; check again on the next user turn")
	}
	m, err := b.message(id)
	if err != nil {
		return nil, err
	}
	if !externalHandoff(p, m) || !b.networkHandoffAllowed(m, p) {
		return object{"message": nil}, nil
	}
	sender, err := b.messagePeer(b.db, m.Sender.ID)
	if err != nil {
		return nil, err
	}
	if !safe(sender) {
		return nil, errors.New(permissionBlockReason("Sender", sender))
	}
	if err = b.event(id, "content_served", p.epoch); err != nil {
		return nil, err
	}
	m.State = "content_served"
	m.SupersededBy = b.successor(m.ID, m.Sender.ID, m.Recipient)
	return object{"message": m}, nil
}

func sessionInstructions(s Session) string {
	if s.DeliveryMode != "manual" {
		return instructions
	}
	return "This AX endpoint cannot wake you automatically. On a user request to check mail, call check_inbox once to receive the next message and list_notifications once for delivery failures. Use list_pending to recover previously fetched messages; never replay work automatically. After sending, end your turn and tell the user that replies require another user turn. Never poll or sleep waiting. " + delegation
}

func sessionTools(s Session) []object {
	var list []object
	for _, tool := range toolList() {
		if tool["name"] == "check_inbox" && s.DeliveryMode != "manual" || tool["name"] == "spawn_agent" && s.Host == "external" {
			continue
		}
		if s.Kind != "relay" {
			delete(tool["inputSchema"].(object)["properties"].(object), "provenance")
		}
		if s.DeliveryMode == "manual" {
			tool["description"] = strings.ReplaceAll(tool["description"].(string), "AX wakes you for replies.", "Replies can be fetched on your next user turn.")
		}
		list = append(list, tool)
	}
	return list
}
