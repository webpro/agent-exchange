package ax

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestClaudeModPostCompactInvalidatesWithoutChangingState(t *testing.T) {
	dir := startTestServer(t)
	s := Session{ID: randomID("agt_"), Secret: randomID(""), Name: "api", Host: "claude", Mesh: testMesh, Native: observedThread, Started: true}
	c := connectDiscoveryPeer(t, dir, s)
	file := filepath.Join(dir, "session.json")
	if err := saveSession(file, s); err != nil {
		t.Fatal(err)
	}
	if err := Hook(dir, file, bytes.NewReader(raw(object{"hook_event_name": "ContextUsage", "session_id": observedThread, "context_usage": object{"context_used_tokens": 32000, "context_window_tokens": 128000, "context_observed_at_ms": 1000}}))); err != nil {
		t.Fatal(err)
	}
	if err := Hook(dir, file, bytes.NewReader(raw(object{"hook_event_name": "PostCompact", "session_id": observedThread}))); err != nil {
		t.Fatal(err)
	}
	var agents []discoveredAgent
	if err := c.call("ax.list", object{}, &agents); err != nil || len(agents) != 1 {
		t.Fatalf("discovery: %+v %v", agents, err)
	}
	a := agents[0]
	if a.State != "ready" || a.ContextUsage == nil || a.UsedTokens != nil || a.WindowTokens == nil || *a.WindowTokens != 128000 || a.ObservedAt <= 1000 {
		t.Fatalf("PostCompact retained usage or changed idle state: %+v usage=%+v", a, a.ContextUsage)
	}
}

func TestClaudeModDoesNotReuseUnknownFiles(t *testing.T) {
	dir := testDir(t)
	first, err := installClaudeMod(dir)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(dir, "outside")
	if err := os.WriteFile(outside, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(first, "skills", "planted-probe"), 0700); err != nil {
		t.Fatal(err)
	}
	planted := filepath.Join(first, "skills", "planted-probe", "SKILL.md")
	if err := os.WriteFile(planted, []byte("planted skill"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(first, "outside-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(first, "hooks", "hooks.json"), 0666); err != nil {
		t.Fatal(err)
	}
	second, err := installClaudeMod(dir)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("another launch reused a potentially active, contaminated mod directory")
	}
	var files []string
	if err := filepath.WalkDir(second, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			rel, _ := filepath.Rel(second, path)
			files = append(files, rel)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(files, ",") != ".claude-plugin/plugin.json,hooks/hooks.json,hooks/register.js" {
		t.Fatalf("unexpected loadable files: %v", files)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "preserve" {
		t.Fatalf("changed outside file: %s %v", data, err)
	}
	if _, err := os.Stat(planted); err != nil {
		t.Fatal("modified another session's mod directory", err)
	}
}

func TestClaudeModVerificationRejectsTampering(t *testing.T) {
	paths := []string{".claude-plugin/plugin.json", "hooks/hooks.json", "hooks/register.js"}
	content := make([][]byte, len(paths))
	for i, path := range paths {
		var err error
		content[i], err = claudeMod.ReadFile("claude_mod/" + path)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, change := range []string{"extra", "directory", "symlink", "permissions", "content", "missing"} {
		t.Run(change, func(t *testing.T) {
			root, err := installClaudeMod(testDir(t))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "hooks", "register.js")
			switch change {
			case "extra":
				err = os.WriteFile(filepath.Join(root, "extra.js"), []byte("planted"), 0600)
			case "directory":
				err = os.Mkdir(filepath.Join(root, "skills"), 0700)
			case "symlink":
				if err = os.Remove(path); err == nil {
					err = os.Symlink("hooks.json", path)
				}
			case "permissions":
				err = os.Chmod(path, 0666)
			case "content":
				data := append([]byte(nil), content[2]...)
				data[0] ^= 1
				err = os.WriteFile(path, data, 0600)
			case "missing":
				err = os.Remove(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = verifyClaudeMod(root, paths, content); err == nil {
				t.Fatalf("accepted %s", change)
			}
		})
	}
}

func TestClaudeModLaunch(t *testing.T) {
	for _, tc := range []struct {
		version string
		loaded  bool
	}{
		{"2.1.317 (Claude Code)", true},
		{"2.1.290 (Claude Code)", true},
		{"2.1.289 (Claude Code)", false},
		{"unknown", false},
	} {
		t.Run(tc.version, func(t *testing.T) {
			dir := startTestServer(t)
			bin := testDir(t)
			out := filepath.Join(bin, "args")
			command := filepath.Join(bin, "command")
			script := "#!/bin/sh\nif [ \"$1\" = --version ]; then printf '%s' " + shellQuote(tc.version) + "; exit 0; fi\nif [ \"$1\" = --help ]; then exit 0; fi\nprintf '%s\\0' \"$@\" > " + shellQuote(out) + "\nprintf '%s' \"$AX_CLAUDE_OBSERVER\" > " + shellQuote(command) + "\n"
			if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			if err := Launch(context.Background(), dir, "claude", []string{"-n", "api", "--plugin-dir", "/user/plugin", "--", "prompt"}); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00")
			var plugin string
			for i, arg := range args {
				if arg == "--plugin-dir" && i+1 < len(args) && args[i+1] != "/user/plugin" {
					plugin = args[i+1]
				}
			}
			if (plugin != "") != tc.loaded {
				t.Fatalf("mod load=%v, want %v: %q", plugin, tc.loaded, args)
			}
			if i := slices.Index(args, "/user/plugin"); i < 1 || args[i-1] != "--plugin-dir" || args[len(args)-2] != "--" || args[len(args)-1] != "prompt" {
				t.Fatalf("changed user arguments: %q", args)
			}
			if !tc.loaded {
				return
			}
			if !strings.HasPrefix(plugin, dir+string(os.PathSeparator)) {
				t.Fatalf("mod depends on a checkout: %s", plugin)
			}
			if _, err := os.Stat(plugin); !os.IsNotExist(err) {
				t.Fatalf("exited launch left its mod directory: %v", err)
			}
			data, err = os.ReadFile(command)
			exe, _ := os.Executable()
			if err != nil || string(data) != exe {
				t.Fatalf("wrong observer binary: %s %v", data, err)
			}
		})
	}
}

func TestClaudeModFailurePreservesNativeLaunch(t *testing.T) {
	for _, refused := range []bool{false, true} {
		dir := startTestServer(t)
		bin := testDir(t)
		out := filepath.Join(bin, "launches")
		exit := "0"
		if refused {
			exit = "23"
		} else if err := os.WriteFile(filepath.Join(dir, "extensions"), []byte("unavailable"), 0600); err != nil {
			t.Fatal(err)
		}
		script := "#!/bin/sh\nif [ \"$1\" = --version ]; then printf '2.1.317 (Claude Code)'; exit 0; fi\nif [ \"$1\" = --help ]; then exit 0; fi\nprintf 'launch\\n' >> " + shellQuote(out) + "\nexit " + exit + "\n"
		if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		err := Launch(context.Background(), dir, "claude", []string{"-n", "api"})
		if (err != nil) != refused {
			t.Fatalf("native refusal=%v: %v", refused, err)
		}
		data, err := os.ReadFile(out)
		if err != nil || string(data) != "launch\n" {
			t.Fatalf("missing launch or sideload retry: %q %v", data, err)
		}
	}
}
