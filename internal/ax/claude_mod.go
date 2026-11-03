package ax

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//go:embed claude_mod/.claude-plugin/plugin.json claude_mod/hooks/hooks.json claude_mod/hooks/register.js
var claudeMod embed.FS

func supportsClaudeMod(ctx context.Context, bin string) bool {
	ctx, cancel := context.WithTimeout(ctx, flagProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--version")
	cmd.WaitDelay = time.Second
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return false
	}
	parts := strings.Split(fields[0], ".")
	if len(parts) != 3 {
		return false
	}
	version := [3]int{}
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return false
		}
		version[i] = value
	}
	return version[0] > 2 || version[0] == 2 && (version[1] > 1 || version[1] == 1 && version[2] >= 290)
}

func installClaudeMod(dir string) (string, error) {
	paths := []string{".claude-plugin/plugin.json", "hooks/hooks.json", "hooks/register.js"}
	content := make([][]byte, len(paths))
	var key strings.Builder
	for i, path := range paths {
		data, err := claudeMod.ReadFile("claude_mod/" + path)
		if err != nil {
			return "", err
		}
		content[i] = data
		key.WriteString(path)
		key.WriteByte(0)
		key.Write(data)
		key.WriteByte(0)
	}
	directory := filepath.Join(dir, "extensions")
	if err := privateDir(directory); err != nil {
		return "", err
	}
	root, err := os.MkdirTemp(directory, "claude_"+digest(key.String())[:16]+"-")
	if err != nil {
		return "", err
	}
	complete := false
	defer func() {
		if !complete {
			os.RemoveAll(root)
		}
	}()
	for i, path := range paths {
		path = filepath.Join(root, path)
		if err := privateDir(filepath.Dir(path)); err != nil {
			return "", err
		}
		if err := os.WriteFile(path, content[i], 0600); err != nil {
			return "", err
		}
	}
	if err := verifyClaudeMod(root, paths, content); err != nil {
		return "", err
	}
	complete = true
	return root, nil
}

func verifyClaudeMod(root string, paths []string, content [][]byte) error {
	expected := make(map[string][]byte, len(paths))
	directories := map[string]bool{".": true}
	for i, path := range paths {
		expected[path] = content[i]
		directories[filepath.Dir(path)] = true
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if !directories[rel] || info.Mode().Perm() != 0700 {
				return fmt.Errorf("unexpected mod directory: %s", rel)
			}
			return nil
		}
		want, ok := expected[rel]
		if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() != int64(len(want)) {
			return fmt.Errorf("unexpected mod file: %s", rel)
		}
		file, err := privateFile(path, syscall.O_RDONLY)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(file, int64(len(want))+1))
		file.Close()
		if err != nil {
			return err
		}
		if !bytes.Equal(data, want) {
			return fmt.Errorf("mod file differs from embedded content: %s", rel)
		}
		delete(expected, rel)
		return nil
	})
	if err == nil && len(expected) != 0 {
		return fmt.Errorf("mod is missing %d embedded files", len(expected))
	}
	return err
}
