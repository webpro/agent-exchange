package ax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var Version = "0.8.2" // Development builds may set this with the Go linker.

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

// AX owns name and external policy. Preserve other arguments, including native subcommands
// and the host's end-of-options separator.
func launchArgs(args []string) (string, []string, error) {
	name, _, native, err := launchOptions(args)
	return name, native, err
}

func launchOptions(args []string) (string, string, []string, error) {
	external := os.Getenv("AX_EXTERNAL")
	var name string
	var native []string
	legacy := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			native = append(native, args[i:]...)
			break
		}
		legacy = legacy || arg == "-name" || strings.HasPrefix(arg, "-name=")
		if arg == "--external" {
			if i+1 == len(args) {
				return "", "", nil, errors.New("external needs a value")
			}
			i++
			external = args[i]
		} else if strings.HasPrefix(arg, "--external=") {
			external = strings.TrimPrefix(arg, "--external=")
		} else if arg == "--name" || arg == "-n" {
			if i+1 == len(args) {
				return "", "", nil, errors.New("name needs a value")
			}
			i++
			name = args[i]
		} else if strings.HasPrefix(arg, "--name=") || strings.HasPrefix(arg, "-n=") {
			name = strings.SplitN(arg, "=", 2)[1]
		} else {
			native = append(native, arg)
		}
	}
	if !validName.MatchString(name) {
		// -name was the AX option through 0.6; it is now an ordinary native argument.
		if legacy {
			return "", "", nil, errors.New("-name is no longer the AX name option; use --name api or -n api")
		}
		return "", "", nil, errors.New("choose an AX name with --name api or -n api")
	}
	if !validExternal(external) {
		return "", "", nil, errors.New("external must be refuse, verified, or all")
	}
	return name, external, native, nil
}

const flagProbeTimeout = 3 * time.Second

// An older harness release may not have the option yet. Help output that is
// missing, unreadable, or slow leaves the native name alone; the probe must not
// be able to fail or stall a launch that would otherwise succeed.
func advertisesFlag(ctx context.Context, bin, flag string) bool {
	ctx, cancel := context.WithTimeout(ctx, flagProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--help")
	// Cancelling signals the harness alone. Without a wait delay, a grandchild
	// holding the output pipe keeps Output blocked past the timeout.
	cmd.WaitDelay = time.Second
	help, err := cmd.Output()
	if err != nil {
		return false
	}
	split := func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == ',' || r == '=' }
	return slices.Contains(strings.FieldsFunc(string(help), split), flag)
}

func withAXOptions(native, options []string) []string {
	for i, arg := range native {
		if arg == "--" {
			out := append([]string(nil), native[:i]...)
			out = append(out, options...)
			return append(out, native[i:]...)
		}
	}
	return append(append([]string(nil), native...), options...)
}

func Launch(ctx context.Context, dir, host string, args []string) error {
	return launch(ctx, dir, host, args, "")
}

func launch(ctx context.Context, dir, host string, args []string, spawnToken string) error {
	name, external, nativeArgs, e := launchOptions(args)
	if e != nil {
		return e
	}
	bypass := false
	if host == "codex" && !codexHelp(nativeArgs) {
		nativeArgs, bypass, e = codexBypassArgs(nativeArgs)
		if e != nil {
			return e
		}
	}
	if host == "grok" || host == "claude" {
		for i, arg := range nativeArgs {
			if arg == "--" {
				break
			}
			if (host == "grok" && (arg == "--always-approve" || arg == "--yolo")) || arg == "--dangerously-skip-permissions" || arg == "--permission-mode=bypassPermissions" || (arg == "--permission-mode" && i+1 < len(nativeArgs) && nativeArgs[i+1] == "bypassPermissions") {
				bypass = true
			}
		}
	}
	nativeBin, e := exec.LookPath(host)
	if e != nil {
		return fmt.Errorf("install and sign in to %s first", host)
	}
	cwd, e := os.Getwd()
	if e != nil {
		return e
	}
	cwd, e = filepath.EvalSymlinks(cwd)
	if e != nil {
		return e
	}
	mesh, e := meshFor(cwd)
	if e != nil {
		return e
	}
	sessions := filepath.Join(dir, "sessions")
	if e = privateDir(sessions); e != nil {
		return e
	}
	path, e := namedSessionPath(sessions, name)
	if e != nil {
		return e
	}
	lock, e := lockFile(path + ".lock")
	if e != nil {
		return fmt.Errorf("agent %q is already running on this machine", name)
	}
	defer lock.Close()
	stateLock, e := lockSession(path)
	if e != nil {
		return e
	}
	defer stateLock.Close()
	s, e := loadSession(path)
	if os.IsNotExist(e) {
		s = Session{ID: randomID("agt_"), Secret: randomID("") + randomID(""), Name: name, Host: host, Mesh: mesh, Workspace: cwd}
	} else if e != nil {
		return e
	}
	if s.Host != host {
		return fmt.Errorf("name %q belongs to %s; choose another name", name, s.Host)
	}
	if s.SpawnToken != "" && s.SpawnToken != spawnToken {
		return fmt.Errorf("agent %q is reserved for a pane launch; inspect ax spawn-status %s", name, name)
	}
	if spawnToken == "" {
		s.SpawnRoot, s.SpawnDepth = randomID("tree_"), 0
	}
	s.SpawnToken = ""
	s.Terminal = detectTerminal()
	s.Workspace, s.Mesh = cwd, mesh
	s.LaunchID = randomID("launch_")
	s.Started = false // This launch must receive its own native SessionStart.
	s.ClaudeStartup = host == "claude" && spawnToken != "" && claudeNeedsStartup(nativeArgs)
	s.BindingError = ""
	s.DeliveryBoundary = os.Getenv("AX_DELIVERY_BOUNDARY")
	if _, e = sessionCapabilities(s); e != nil {
		return e
	}
	if s.ClaudePending != "" && !missingClaudeTranscript(s.ClaudePending) {
		s.ClaudePending = ""
	}
	s.External = external
	s.AllowBypass = bypassOptIn(s.AllowBypass, bypass)
	if e = saveSession(path, s); e != nil {
		return e
	}
	stateLock.Close()
	if e = ensureBroker(dir); e != nil {
		return e
	}
	c, e := dial(socketPath(dir))
	if e != nil {
		return e
	}
	e = c.call("ax.enroll", s, nil)
	c.close()
	if e != nil {
		return e
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	env := append(os.Environ(), "AX_HOME="+dir, "AX_SESSION_FILE="+path, "AX_LAUNCH_ID="+s.LaunchID)
	// Native daemons may run hooks without the launching shell environment.
	hookCommand := "AX_HOME=" + shellQuote(dir) + " AX_SESSION_FILE=" + shellQuote(path) + " AX_LAUNCH_ID=" + shellQuote(s.LaunchID) + " " + shellQuote(exe) + " hook"
	startupErrors := make(chan error, 1)
	var argv []string
	if host == "claude" {
		config := object{"mcpServers": object{"ax": object{"command": exe, "args": []string{"bridge"}, "alwaysLoad": true, "env": object{"AX_HOME": dir, "AX_SESSION_FILE": path, "AX_LAUNCH_ID": s.LaunchID}}}}
		hook := object{"type": "command", "command": hookCommand, "timeout": 3}
		hooks := object{}
		for _, event := range []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PermissionRequest", "PostToolUse", "Stop"} {
			hooks[event] = []object{{"hooks": []object{hook}}}
		}
		// Agent view can re-host Claude without its channel opt-in, leaving MCP
		// connected but unable to wake the model (anthropics/claude-code#90160).
		settings := object{"hooks": hooks, "disableAgentView": true}
		allowed := []string{}
		for _, tool := range toolSpecs {
			// Launching a process retains the harness's normal tool approval.
			if tool.name != "spawn_agent" {
				allowed = append(allowed, "mcp__ax__"+tool.name)
			}
		}
		argv = []string{"--mcp-config", string(raw(config)), "--settings", string(raw(settings)), "--dangerously-load-development-channels", "server:ax", "--allowedTools", strings.Join(allowed, ","), "--append-system-prompt", instructions}
		if len(nativeArgs) == 0 && s.Native != "" {
			nativeArgs = []string{"--resume", s.Native}
			if s.ClaudePending != "" {
				// SessionStart can run before Claude creates a transcript. Reuse
				// that exact UUID only while its recorded path is still absent.
				nativeArgs[0] = "--session-id"
				fmt.Fprintln(os.Stderr, "Claude has not saved this conversation yet; continuing with the same session ID.")
			}
		}
		fmt.Fprintf(os.Stderr, "Agent Exchange · %s\nConfirm Claude's local development Channel prompt when it appears.\nClaude agent view is disabled for this session to preserve AX channel delivery.\n", name)
	} else if host == "codex" {
		b, e := exec.Command(nativeBin, "queue", "--help").Output()
		if e != nil || !strings.Contains(string(b), "--thread") {
			return errors.New("AX needs Codex with native queue support; update Codex first")
		}
		config := []string{"mcp_servers.ax.command=" + strconv.Quote(exe), "mcp_servers.ax.args=[\"bridge\"]", "mcp_servers.ax.env.AX_HOME=" + strconv.Quote(dir), "mcp_servers.ax.env.AX_SESSION_FILE=" + strconv.Quote(path), "mcp_servers.ax.env.AX_LAUNCH_ID=" + strconv.Quote(s.LaunchID), "mcp_servers.ax.enabled=true"}
		if !codexHelp(nativeArgs) {
			remote, cleanup, err := startCodex(ctx, dir, path, nativeBin, append(codexConfigOverrides(nativeArgs), config...))
			if err != nil {
				return err
			}
			defer cleanup()
			s.CodexRemote = remote
			if err = saveSession(path, s); err != nil {
				return err
			}
			remote, cleanup, err = codexSessionSocket(ctx, dir, path, remote, bypass, startupErrors)
			if err != nil {
				return err
			}
			defer cleanup()
			argv = append(argv, "--remote", remote)
		}
		for _, v := range config {
			argv = append(argv, "-c", v)
		}
		if len(nativeArgs) == 0 && s.Native != "" {
			nativeArgs = []string{"resume", s.Native}
		}
		fmt.Fprintf(os.Stderr, "Agent Exchange · %s\nCodex connects AX automatically after selecting your session.\n", name)
	} else {
		adapter := harnesses[host]
		if adapter.prepare == nil {
			return fmt.Errorf("unsupported harness %q", host)
		}
		var cleanup func()
		nativeArgs, env, cleanup, e = adapter.prepare(ctx, dir, path, nativeBin, s, nativeArgs, env, startupErrors)
		if e != nil {
			return e
		}
		defer cleanup()
		fmt.Fprintf(os.Stderr, "Agent Exchange · %s · %s\n", name, host)
	}
	// Show the AX name in the harness's own session UI. launchArgs already removed
	// every user-supplied copy of the option, so this cannot pass it twice.
	if flag := harnesses[host].nameFlag; flag != "" && advertisesFlag(ctx, nativeBin, flag) {
		argv = append(argv, flag, name)
	}
	// Keep injected Codex configuration in the subcommand's option scope, where
	// it survives resume alongside user-supplied subcommand config overrides.
	argv = withAXOptions(nativeArgs, argv)
	if s.ClaudeStartup {
		if len(nativeArgs) == 0 || nativeArgs[len(nativeArgs)-1] != "--" {
			argv = append(argv, "--")
		}
		argv = append(argv, claudeStartupPrompt)
	}
	cmd := exec.CommandContext(ctx, nativeBin, argv...)
	cmd.Env = env
	// The harness retains the name lock even if its AX launcher crashes. Pi
	// inherits this descriptor through execNative instead of starting a child.
	cmd.ExtraFiles = []*os.File{lock}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if harnesses[host].nativeLaunch {
		// Replace AX with the harness. Its optional extension owns the messaging
		// child, so AX has no parent or terminal transport in the session lifetime.
		return execNative(nativeBin, argv, cmd.Environ(), lock)
	}
	// Let the real harness own terminal input and its normal signal handling.
	// AX never emulates keystrokes or reads/mutates its private transcript files.
	ignored := make(chan os.Signal, 2)
	signal.Notify(ignored, os.Interrupt)
	defer signal.Stop(ignored)
	return runHost(cmd, startupErrors)
}

func execNative(binary string, args, env []string, lock *os.File) error {
	flags, err := unix.FcntlInt(lock.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return err
	}
	if _, err = unix.FcntlInt(lock.Fd(), unix.F_SETFD, flags&^unix.FD_CLOEXEC); err != nil {
		return err
	}
	return syscall.Exec(binary, append([]string{binary}, args...), env)
}

// Messaging is optional: report adapter failures without terminating the host.
func runHost(cmd *exec.Cmd, startupErrors <-chan error) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	log := cmd.Stderr
	if log == nil {
		log = os.Stderr
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	for {
		select {
		case err := <-done:
			return err
		case err, ok := <-startupErrors:
			if !ok {
				startupErrors = nil
			} else if err != nil {
				fmt.Fprintln(log, "AX messaging unavailable:", err)
			}
		}
	}
}

// Check only native-provided path metadata, never transcript contents. Existing
// files (even empty ones), unknown legacy state, and stat errors require resume.
func missingClaudeTranscript(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	_, err := os.Lstat(path)
	return os.IsNotExist(err)
}

func Hook(dir, file string, in io.Reader, launch ...string) error {
	expected := os.Getenv("AX_LAUNCH_ID")
	if len(launch) > 0 {
		expected = launch[0]
	}
	var input struct {
		Session    string `json:"session_id"`
		Event      string `json:"hook_event_name"`
		Permission string `json:"permission_mode"`
		Tool       string `json:"tool_name"`
		File       string `json:"session_file"`
		Source     string `json:"source"`
		Transcript string `json:"transcript_path"`
	}
	if err := json.NewDecoder(io.LimitReader(in, maxFrame)).Decode(&input); err != nil {
		return err
	}
	if !validNative(input.Session) {
		return errors.New("invalid native session ID in lifecycle hook")
	}
	lock, e := lockSession(file)
	if e != nil {
		return e
	}
	defer lock.Close()
	s, e := loadSession(file)
	if e != nil {
		return e
	}
	if expected != s.LaunchID {
		return errors.New("AX hook belongs to an earlier launch")
	}
	if s.BindingError != "" {
		return errors.New(s.BindingError)
	}
	if input.Event == "SessionStart" && (s.Native == "" || (!s.Started && s.LaunchID != "")) {
		if s.Native != input.Session {
			s.ClaudePending = ""
			s.NativeFile = ""
		}
		s.Native = input.Session
		if s.Host == "claude" && input.Source == "startup" && missingClaudeTranscript(input.Transcript) {
			s.ClaudePending = input.Transcript
		}
	}
	if input.Session != s.Native {
		if input.Event == "SessionStart" {
			conflict := bindingConflict(s, input.Session)
			s.Started, s.BindingError = false, conflict.Error()
			if e = saveSession(file, s); e != nil {
				return e
			}
			lock.Close()
			// Tell an already-connected broker to hold mail. The file remains the
			// durable error source even if the broker is temporarily unavailable.
			if c, err := dial(socketPath(dir)); err == nil {
				defer c.close()
				_ = c.call("ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": s.Native, "launch_id": s.LaunchID, "state": "blocked", "binding_error": s.BindingError}, nil)
			}
			return conflict
		}
		return nil
	}
	persisted := s.ClaudePending != "" && !missingClaudeTranscript(s.ClaudePending)
	if persisted {
		s.ClaudePending = ""
	}
	if input.Event == "SessionStart" {
		if s.Host == "pi" && input.File != "" && filepath.IsAbs(input.File) {
			s.NativeFile = input.File
		}
		s.Started = true
	}
	if input.Event == "SessionStart" || persisted {
		if e = saveSession(file, s); e != nil {
			return e
		}
	}
	lock.Close()
	state := "busy"
	var cause *stateCause
	switch input.Event {
	case "SessionStart":
		state = "ready"
		if s.Host == "claude" && input.Source == "compact" {
			state = ""
		}
	case "Stop":
		state = "ready"
	case "PermissionRequest":
		state = "blocked"
		cause = &stateCause{Kind: "permission", Tool: input.Tool}
	}
	if input.Tool == "AskUserQuestion" {
		state = "blocked"
		cause = nil
		if input.Event != "PostToolUse" { // The answer has arrived; the state stays until the next event.
			cause = &stateCause{Kind: "question"}
		}
	}
	c, e := dial(socketPath(dir))
	if e != nil {
		return e
	}
	defer c.close()
	return c.call("ax.lifecycle", object{"agent_id": s.ID, "secret": s.Secret, "native_session_id": input.Session, "launch_id": s.LaunchID, "permission_mode": input.Permission, "state": state, "state_cause": cause}, nil)
}

func bindingConflict(s Session, actual string) error {
	return fmt.Errorf("AX name %q belongs to conversation %s, but %s selected %s. Exit this session, then run ax %s -n %s to resume the saved conversation, or relaunch this conversation with an unused AX name. Saved mail stays with the original name", s.Name, s.Native, s.Host, actual, s.Host, s.Name)
}

func Main(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Println(`Agent Exchange — connect your local coding agents.

  ax claude --name api
  ax codex --name web
  ax grok --name reviewer
  ax opencode --name editor
  ax pi --name worker

AX owns the name option, --name or -n. Other arguments go to the native
harness, which also receives the name when it can display one.
  ax claude --name api -r "session-name"
  ax codex --name web resume "session-name"
  ax grok --name reviewer -r "session-name"
  ax opencode --name editor -s SESSION_ID

Ask either agent to message another by name.

  ax agents                     List local agents
  ax attach -n NAME -s SESSION   Join an existing runtime over MCP stdio
  ax verify NAME                Verify a request and reply with an agent
  ax inbox [-a NAME] [-t THREAD_ID] [-s STATE]  Inspect local delivery timelines
  ax spawn codex --name worker   Open a named peer in a new terminal pane
  ax spawn-status NAME          Inspect a previous pane launch
  ax status MESSAGE_ID          Inspect delivery receipts
  ax resolve MESSAGE_ID abandon Release a stuck message without redelivery
  ax policy NAME hold           Pause incoming mail (accept/hold/refuse)
  ax doctor                     Diagnose harnesses, Channels, and local discovery
  ax network                    Connect paired Tailscale machines (development)
  ax external NAME POLICY       Set external policy: refuse, verified, all
  ax version                    Show the AX version`)
		return nil
	}
	if args[0] == "version" {
		fmt.Println(Version)
		return nil
	}
	if args[0] == "spawn-run" {
		if len(args) != 4 {
			return errors.New("invalid internal pane launch")
		}
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer cancel()
		return runSpawn(ctx, args[1], args[2], args[3])
	}
	dir, e := dataDir()
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer cancel()
	// Do not export GOMAXPROCS or apply inherited limits to native harnesses.
	switch args[0] {
	case "serve", "bridge", "hook", "inbox", "attach", "verify", "network":
		runtime.GOMAXPROCS(1)
	}
	switch args[0] {
	case "network":
		if err := resourcePause(dir, time.Now()); err != nil {
			return err
		}
		stopResources := watchResources(context.Background(), dir, cancel, func() { os.Exit(75) })
		defer stopResources()
		return networkCLI(ctx, dir, args[1:], os.Stdin, os.Stdout)
	case "attach":
		inputInfo, err := os.Stdin.Stat()
		if err != nil {
			return err
		}
		if inputInfo.Mode()&os.ModeCharDevice != 0 {
			return errors.New("configure ax attach as an MCP child with piped stdin; it is not an interactive terminal command")
		}
		// NewFile registers a nonblocking pipe with Go's poller, allowing Close
		// to interrupt an idle MCP read when SIGTERM cancels this child.
		inputFD := os.Stdin.Fd()
		flags, err := unix.FcntlInt(inputFD, unix.F_GETFL, 0)
		if err != nil {
			return err
		}
		defer unix.FcntlInt(inputFD, unix.F_SETFL, flags)
		fd, err := syscall.Dup(int(inputFD))
		if err != nil {
			return err
		}
		syscall.CloseOnExec(fd)
		if err = syscall.SetNonblock(fd, true); err != nil {
			syscall.Close(fd)
			return err
		}
		input := os.NewFile(uintptr(fd), "AX MCP input")
		defer input.Close()
		return attach(ctx, dir, args[1:], input, os.Stdout, func() { os.Exit(75) })
	case "verify":
		if len(args) != 2 || !validName.MatchString(args[1]) {
			return errors.New("usage: ax verify NAME")
		}
		return verifyAgent(ctx, dir, args[1], os.Stdout)
	case "spawn":
		result, err := spawnCLI(ctx, dir, args[1:])
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	case "spawn-status":
		if len(args) != 2 || !validName.MatchString(args[1]) {
			return errors.New("usage: ax spawn-status NAME")
		}
		result, err := spawnStatus(ctx, dir, args[1])
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	case "serve":
		if err := resourcePause(dir, time.Now()); err != nil {
			return err
		}
		// Keep the monitor alive after it cancels Serve, so a hot shutdown
		// cannot disable its own fallback. The defer stops it when Serve returns.
		stopResources := watchResources(context.Background(), dir, cancel, func() { os.Exit(75) })
		defer stopResources()
		go watchDiagnosticLog(ctx, dir, "broker.log")
		return Serve(ctx, dir)
	case "claude", "codex", "grok", "opencode", "pi":
		return Launch(ctx, dir, args[0], args[1:])
	case "bridge":
		return runBridge(ctx, dir, os.Getenv("AX_SESSION_FILE"), os.Stdin, os.Stdout, func() { os.Exit(75) })
	case "hook":
		// A resource pause must not block a native permission or lifecycle hook.
		if err := resourcePause(dir, time.Now()); err != nil {
			return nil
		}
		defer func() {
			if cpu, err := processCPU(); err == nil {
				_, _ = reportCPU(dir, time.Now(), cpu)
			}
		}()
		return Hook(dir, os.Getenv("AX_SESSION_FILE"), os.Stdin)
	case "inbox":
		if err := resourcePause(dir, time.Now()); err != nil {
			return err
		}
		stopResources := watchResources(ctx, dir, cancel, nil)
		defer stopResources()
		filter, err := parseInboxFilter(args[1:])
		if err != nil {
			return err
		}
		return InboxFiltered(ctx, dir, filter, os.Stdin, os.Stdout)
	case "doctor":
		doctor(ctx, dir, os.Stdout)
		return nil
	case "agents", "status", "policy", "external", "resolve":
		if e = ensureBroker(dir); e != nil {
			return e
		}
		c, e := dial(socketPath(dir))
		if e != nil {
			return e
		}
		defer c.close()
		var result any
		switch args[0] {
		case "agents":
			var agents []discoveredAgent
			e = c.call("ax.inspect", object{}, &agents)
			if e != nil {
				return e
			}
			if len(agents) == 0 {
				fmt.Println("No AX agents yet. Launch a named session with ax HARNESS --name NAME.")
			}
			printAgents(os.Stdout, agents, time.Now())
			return nil
		case "status":
			if len(args) != 2 {
				return errors.New("usage: ax status MESSAGE_ID")
			}
			e = c.call("ax.inspect_message", object{"message_id": args[1]}, &result)
		case "resolve":
			if len(args) != 3 || args[2] != "abandon" {
				return errors.New("usage: ax resolve MESSAGE_ID abandon")
			}
			e = c.call("ax.abandon", object{"message_id": args[1]}, &result)
		case "external":
			if len(args) != 3 {
				return errors.New("usage: ax external NAME refuse|verified|all")
			}
			e = c.call("ax.external", object{"target": args[1], "external": args[2]}, &result)
		case "policy":
			if len(args) != 3 {
				return errors.New("usage: ax policy NAME accept|hold|refuse")
			}
			e = c.call("ax.policy", object{"target": args[1], "policy": args[2]}, &result)
		}
		if e != nil {
			return e
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	return fmt.Errorf("unknown command %q; run ax help", args[0])
}
