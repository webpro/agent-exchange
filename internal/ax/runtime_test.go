package ax

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeDirectoryIgnoresItsStateInGit(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			repo := testDir(t)
			git := func(args ...string) string {
				t.Helper()
				cmd := exec.Command("git", args...)
				cmd.Dir = repo
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v\n%s", args, err, out)
				}
				return string(out)
			}
			git("init", "--quiet")
			dir := filepath.Join(repo, ".ax")
			if existing {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "mailbox.db"), []byte("existing mailbox"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("AX_HOME", dir)
			if _, err := dataDir(); err != nil {
				t.Fatal(err)
			}
			if err := privateDir(filepath.Join(dir, "sessions")); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(filepath.Join(dir, "sessions", ".gitignore")); !os.IsNotExist(err) {
				t.Fatalf("nested directory has an ignore file: %v", err)
			}
			for _, file := range []string{"mailbox.db", "sessions/api.json"} {
				if err := os.WriteFile(filepath.Join(dir, file), []byte("private state"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if got := git("check-ignore", ".ax/.gitignore", ".ax/mailbox.db", ".ax/sessions/api.json"); got != ".ax/.gitignore\n.ax/mailbox.db\n.ax/sessions/api.json\n" {
				t.Fatalf("ignored paths = %q", got)
			}
			git("add", "-A")
			if got := git("ls-files"); got != "" {
				t.Fatalf("runtime files staged: %q", got)
			}
			ignore := filepath.Join(dir, ".gitignore")
			if data, err := os.ReadFile(ignore); err != nil || string(data) != "*\n" {
				t.Fatalf(".gitignore = %q, %v", data, err)
			}
			if info, err := os.Stat(ignore); err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf(".gitignore permissions: %v, %v", info, err)
			}
		})
	}
}

func TestRuntimeDirectoryPreservesExistingGitignore(t *testing.T) {
	for _, content := range []string{"", "mailbox.db\n!sessions/\n"} {
		t.Run(content, func(t *testing.T) {
			dir := testDir(t)
			ignore := filepath.Join(dir, ".gitignore")
			if err := os.WriteFile(ignore, []byte(content), 0400); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AX_HOME", dir)
			if _, err := dataDir(); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(ignore); err != nil || string(data) != content {
				t.Fatalf("existing .gitignore changed: %q, %v", data, err)
			}
			if info, err := os.Stat(ignore); err != nil || info.Mode().Perm() != 0400 {
				t.Fatalf("existing permissions changed: %v, %v", info, err)
			}
		})
	}
}

func TestRuntimeDirectoryAllowsGitignoreCreationFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}
	dir := testDir(t)
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0700) })
	t.Setenv("AX_HOME", dir)
	if got, err := dataDir(); err != nil || got != dir {
		t.Fatalf("read-only runtime directory = %q, %v", got, err)
	}
}

func TestRuntimeDirectorySkipsRepositoryRoot(t *testing.T) {
	for _, kind := range []string{"directory", "file"} {
		t.Run(kind, func(t *testing.T) {
			dir := testDir(t)
			if kind == "directory" {
				if out, err := exec.Command("git", "init", "--quiet", dir).CombinedOutput(); err != nil {
					t.Fatalf("git init: %v\n%s", err, out)
				}
			} else if err := os.WriteFile(filepath.Join(dir, ".git"), []byte("gitdir: elsewhere\n"), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AX_HOME", dir)
			if _, err := dataDir(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(filepath.Join(dir, ".gitignore")); !os.IsNotExist(err) {
				t.Fatalf("repository root has an ignore file: %v", err)
			}
			if kind == "directory" {
				if err := os.WriteFile(filepath.Join(dir, "app.go"), []byte("package main\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if out, err := exec.Command("git", "-C", dir, "add", "-A").CombinedOutput(); err != nil {
					t.Fatalf("git add: %v\n%s", err, out)
				}
				if out, err := exec.Command("git", "-C", dir, "ls-files").CombinedOutput(); err != nil || string(out) != "app.go\n" {
					t.Fatalf("staged source = %q, %v", out, err)
				}
			}
		})
	}
}

func TestRuntimeDirectoryReadOnlyCLI(t *testing.T) {
	if command := os.Getenv("AX_TEST_RUNTIME_READ_COMMAND"); command != "" {
		if err := Main(strings.Fields(command)); err != nil {
			fmt.Fprintln(os.Stderr, "ax:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}
	dir := startTestServer(t)
	t.Setenv("PATH", testDir(t))
	from := connectDiscoveryPeer(t, dir, Session{ID: "agt_web", Secret: randomID("") + randomID(""), Name: "web", Host: "codex", Mesh: testMesh, Native: uuid()})
	admin, err := dial(socketPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.close()
	if err := admin.call("ax.enroll", Session{ID: "agt_api", Secret: randomID("") + randomID(""), Name: "api", Host: "codex", Mesh: testMesh, Native: uuid()}, nil); err != nil {
		t.Fatal(err)
	}
	var message Message
	if err := from.call("ax.send", object{"target": "api", "text": "queued task", "client_message_id": "read-only"}, &message); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0700) })
	for _, tc := range []struct {
		command string
		code    int
		stdout  string
		stderr  string
	}{
		{"doctor", 0, "Broker: reachable, 2 registered agents\n", ""},
		{"agents", 0, "api                codex    unstarted", ""},
		{"status " + message.ID, 0, `"status": "queued"`, ""},
		{"status msg_missing", 1, "", "ax: message not found\n"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRuntimeDirectoryReadOnlyCLI$")
			cmd.Env = append(os.Environ(), "AX_HOME="+dir, "AX_TEST_RUNTIME_READ_COMMAND="+tc.command)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != tc.code || !strings.Contains(stdout.String(), tc.stdout) || stderr.String() != tc.stderr {
				t.Fatalf("%s: %v; stdout=%q stderr=%q", tc.command, err, &stdout, &stderr)
			}
			t.Logf("exit=%d stdout=%q stderr=%q", cmd.ProcessState.ExitCode(), &stdout, &stderr)
		})
	}
	if _, err := os.Lstat(filepath.Join(dir, ".gitignore")); !os.IsNotExist(err) {
		t.Fatalf("read-only fixture has an ignore file: %v", err)
	}
}
