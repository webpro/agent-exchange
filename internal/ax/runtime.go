//go:build darwin || linux

package ax

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type Session struct {
	LaunchID         string          `json:"launch_id,omitempty"`
	Kind             string          `json:"kind,omitempty"`
	External         string          `json:"external,omitempty"`
	ID               string          `json:"agent_id"`
	Secret           string          `json:"secret"`
	Name             string          `json:"name"`
	Host             string          `json:"host"`
	Mesh             string          `json:"mesh"`
	Workspace        string          `json:"workspace"`
	Native           string          `json:"native_session_id"`
	NativeFile       string          `json:"native_session_file,omitempty"`
	ClaudePending    string          `json:"claude_pending_transcript,omitempty"`
	ClaudeStartup    bool            `json:"claude_startup_pending,omitempty"`
	Started          bool            `json:"started"`
	BindingError     string          `json:"binding_error,omitempty"`
	AllowBypass      bool            `json:"allow_bypass"`
	CodexRemote      string          `json:"codex_remote,omitempty"`
	AdapterSocket    string          `json:"adapter_socket,omitempty"`
	DeliveryMode     string          `json:"delivery_mode,omitempty"`
	DeliveryBoundary string          `json:"delivery_boundary,omitempty"`
	Terminal         terminalContext `json:"terminal,omitempty"`
	SpawnRoot        string          `json:"spawn_root,omitempty"`
	SpawnDepth       int             `json:"spawn_depth,omitempty"`
	SpawnToken       string          `json:"spawn_token,omitempty"`
}

// A named endpoint keeps its explicit AX opt-in across resume. A present
// environment setting replaces the saved choice; native bypass flags still
// opt in as they do on a new launch.
func bypassOptIn(saved, explicit bool) bool {
	if value, set := os.LookupEnv("AX_ALLOW_BYPASS"); set {
		return explicit || value == "1"
	}
	return explicit || saved
}

func randomID(prefix string) string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return prefix + hex.EncodeToString(b)
}
func uuid() string {
	s := randomID("")
	return s[:8] + "-" + s[8:12] + "-4" + s[13:16] + "-a" + s[17:20] + "-" + s[20:]
}
func digest(s string) string { v := sha256.Sum256([]byte(s)); return hex.EncodeToString(v[:]) }
func dataDir() (string, error) {
	p := os.Getenv("AX_HOME")
	if p == "" {
		h, e := os.UserHomeDir()
		if e != nil {
			return "", e
		}
		p = filepath.Join(h, ".ax")
	}
	p, e := filepath.Abs(p)
	if e != nil {
		return "", e
	}
	if e = privateDir(p); e != nil {
		return "", e
	}
	ignoreRuntimeState(p)
	return p, nil
}
func ignoreRuntimeState(dir string) {
	if _, err := os.Lstat(filepath.Join(dir, ".git")); !os.IsNotExist(err) {
		return
	}
	path := filepath.Join(dir, ".gitignore")
	f, err := privateFile(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY)
	if err != nil {
		return
	}
	_, err = f.WriteString("*\n")
	if errors.Join(err, f.Close()) != nil {
		os.Remove(path)
	}
}
func privateDir(p string) error {
	// Check existing components before creating anything. OS-provided /tmp aliases
	// are resolved only by callers that deliberately chose them.
	for q := p; ; q = filepath.Dir(q) {
		info, e := os.Lstat(q)
		if e == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink in runtime path: %s", q)
			}
			owner := info.Sys().(*syscall.Stat_t).Uid
			if owner != 0 && owner != uint32(os.Getuid()) {
				return fmt.Errorf("runtime path component has another owner: %s", q)
			}
			if info.Mode().Perm()&0022 != 0 && info.Mode()&os.ModeSticky == 0 {
				return fmt.Errorf("runtime path component is writable by others: %s", q)
			}
			if !info.IsDir() {
				return fmt.Errorf("not a directory: %s", q)
			}
		} else if !os.IsNotExist(e) {
			return e
		}
		if q == filepath.Dir(q) {
			break
		}
	}
	if e := os.MkdirAll(p, 0700); e != nil {
		return e
	}
	info, e := os.Lstat(p)
	if e != nil {
		return e
	}
	st := info.Sys().(*syscall.Stat_t)
	if st.Uid != uint32(os.Getuid()) || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("runtime directory must be owned by you with mode 0700: %s", p)
	}
	return nil
}
func privateFile(p string, flags int) (*os.File, error) {
	fd, e := syscall.Open(p, flags|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), p)
	st, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, e
	}
	if !st.Mode().IsRegular() || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) || st.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, fmt.Errorf("unsafe private file: %s", p)
	}
	return f, nil
}
func lockFile(p string) (*os.File, error) {
	f, e := privateFile(p, syscall.O_CREAT|syscall.O_RDWR)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, errors.New("already running")
	}
	return f, nil
}
func saveSession(p string, s Session) error {
	return savePrivateJSON(p, s)
}

// Hooks and MCP metadata can both finish startup. Serialize their file updates
// separately from the launcher's lifetime lock, with a bounded contention wait.
func lockSession(path string) (*os.File, error) {
	if path == "" {
		return nil, errors.New("AX session file is missing")
	}
	deadline := time.Now().Add(time.Second)
	for {
		f, err := lockFile(path + ".state.lock")
		if err == nil {
			return f, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("AX session state is busy or unavailable: %w", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
func savePrivateJSON(p string, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	tmp := p + "." + randomID("")
	f, e := privateFile(tmp, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY)
	if e != nil {
		return e
	}
	defer os.Remove(tmp)
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	return os.Rename(tmp, p)
}
func loadSession(p string) (Session, error) {
	var s Session
	f, e := privateFile(p, syscall.O_RDONLY)
	if e != nil {
		return s, e
	}
	defer f.Close()
	e = json.NewDecoder(f).Decode(&s)
	return s, e
}
func meshFor(cwd string) (string, error) {
	root, e := filepath.EvalSymlinks(cwd)
	if e != nil {
		return "", e
	}
	cmd := exec.Command("git", "-C", root, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if b, e := cmd.Output(); e == nil {
		root = strings.TrimSpace(string(b))
		root, e = filepath.EvalSymlinks(root)
		if e != nil {
			return "", e
		}
	}
	return digest(root)[:24], nil
}
func socketPath(dir string) string { return filepath.Join(dir, "broker.sock") }
func ensureBroker(dir string) error {
	if err := resourcePause(dir, time.Now()); err != nil {
		return err
	}
	if c, e := dial(socketPath(dir)); e == nil {
		defer c.close()
		return c.call("ax.ping", object{}, nil)
	}
	// Serialize startup across callers. Keep this separate from the broker's
	// lifetime lock; never unlink either inode or transfer ownership by pathname.
	deadline := time.Now().Add(5 * time.Second)
	var startup *os.File
	for time.Now().Before(deadline) {
		var err error
		startup, err = lockFile(filepath.Join(dir, "broker-start.lock"))
		if err == nil {
			break
		}
		if c, e := dial(socketPath(dir)); e == nil {
			err = c.call("ax.ping", object{}, nil)
			c.close()
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	if startup == nil {
		return errors.New("AX broker startup is already in progress")
	}
	defer startup.Close()
	// Another caller may have started it while we waited for the startup lock.
	if c, e := dial(socketPath(dir)); e == nil {
		defer c.close()
		return c.call("ax.ping", object{}, nil)
	}
	owner, err := lockFile(filepath.Join(dir, "broker.lock"))
	if err != nil {
		return fmt.Errorf("AX broker owns its lock but is not accepting connections: %w", err)
	}
	owner.Close()
	if err := resourcePause(dir, time.Now()); err != nil {
		return err
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	log, e := openDiagnosticLog(dir, "broker.log")
	if e != nil {
		return e
	}
	defer log.Close()
	cmd := exec.Command(exe, "serve")
	cmd.Env = append(os.Environ(), "AX_HOME="+dir)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if e = cmd.Start(); e != nil {
		return e
	}
	go cmd.Wait()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, e := dial(socketPath(dir)); e == nil {
			e = c.call("ax.ping", object{}, nil)
			c.close()
			if e == nil {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("broker did not start; inspect %s", filepath.Join(dir, "broker.log"))
}
func listenLocal(dir string) (net.Listener, *os.File, error) {
	lock, e := lockFile(filepath.Join(dir, "broker.lock"))
	if e != nil {
		return nil, nil, e
	}
	path := socketPath(dir)
	if info, e := os.Lstat(path); e == nil {
		if info.Mode()&os.ModeSocket == 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
			lock.Close()
			return nil, nil, errors.New("unsafe existing socket")
		}
		if e = os.Remove(path); e != nil {
			lock.Close()
			return nil, nil, e
		}
	} else if !os.IsNotExist(e) {
		lock.Close()
		return nil, nil, e
	}
	l, e := net.Listen("unix", path)
	if e != nil {
		lock.Close()
		return nil, nil, e
	}
	if e = os.Chmod(path, 0600); e != nil {
		l.Close()
		lock.Close()
		return nil, nil, e
	}
	return l, lock, nil
}

// Reuse pre-0.3 identities regardless of launch directory. New names have one
// machine-wide file and lock; separate AX_HOME directories remain isolated.
func namedSessionPath(dir, name string) (string, error) {
	candidates, err := filepath.Glob(filepath.Join(dir, strings.Repeat("[0-9a-f]", 24)+"_"+name+".json"))
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, name+".json")
	if _, err := os.Lstat(path); err == nil {
		candidates = append(candidates, path)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if len(candidates) > 1 {
		return "", fmt.Errorf("multiple saved agents named %q; choose a unique AX name", name)
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	return path, nil
}
