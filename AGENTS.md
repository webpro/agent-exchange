# Install Agent Exchange (AX)

Follow this guide when the user asks you to install or update Agent Exchange. AX connects named Claude Code, Codex CLI, Grok Build, OpenCode, and Pi sessions on the same machine. Its command is `ax`. Optional Tailscale pairing connects agents across explicitly paired machines.

## Requirements

- macOS 13 or later, or Linux, on ARM64 or Intel/AMD x86_64.
- On Windows, run inside WSL 2. AX and the coding harnesses must run in the same Linux distribution.
- A POSIX shell, curl, tar, and either sha256sum or shasum. Install missing utilities with the user's package manager as appropriate.

AX uses prebuilt binaries. Go, Make, Node, a C compiler, a GitHub account, and GitHub CLI are not installation prerequisites. Linux binaries include SQLite and are statically linked.

## Install or update

Run as the current user:

```sh
curl -fsSL https://useax.dev/install.sh | sh
```

The installer selects the latest release for the OS and CPU, verifies its SHA-256 checksum and version, and atomically installs `ax` into `$HOME/.local/bin`. Failed downloads and checksum checks preserve the previous binary. If necessary, the installer adds that directory to the user's shell configuration.

Make AX available in the current POSIX shell and verify the installation:

```sh
export PATH="$HOME/.local/bin:$PATH"
ax version
ax doctor
```

For an interactive fish shell, open a new terminal after installation, or use `fish_add_path "$HOME/.local/bin"` instead of `export`.

Report the installed version and the harnesses found by doctor. Missing harnesses do not mean AX installation failed. Install and sign in to the coding harnesses separately, according to the user's request.

## Start communicating

Open two interactive terminals, in any repositories, and launch one session in each:

```sh
ax claude --name api
ax codex --name web
```

In Claude's window, submit any prompt first so AX can learn its permission mode and release held mail.

Ask Codex: **“Ask api whether the schema is ready.”** Both agents connect automatically. Names work across repositories for the same OS user.

Other supported harnesses join with `ax grok --name worker`, `ax opencode --name editor`, or `ax pi --name worker`. AX consumes the name option and passes other arguments through to the native harness. Use the native resume syntax to adopt an existing conversation:

```sh
ax claude --name api -r "session-name"
ax codex --name web resume "session-name"
```

Launching the same AX name without additional arguments resumes its saved conversation. Existing sessions keep running during an AX update; relaunch them to use the new binary. Saved conversations and AX mailbox state are preserved.

## Updating running sessions

Installing a new binary does not replace a running broker or bridge. New features need the new broker as well as relaunched sessions. To finish an update from an earlier release:

1. Let active work finish and close the AX-launched sessions normally. Keep their names for resume.
2. Identify the current user's `ax serve` process that owns the socket in the relevant AX state directory, using the system's process and socket inspection tools. The default socket is `$HOME/.ax/broker.sock`; `AX_HOME` selects another directory.
3. Stop only that verified broker PID with `kill -TERM BROKER_PID` and wait for it to exit before proceeding. Do not signal a launcher, bridge, harness, or process group. If its identity cannot be verified, leave it running and report that the broker update is still pending.
4. Before starting the new broker, make a private backup of the stopped runtime's mailbox files and saved sessions. The default directory is `$HOME/.ax`; `AX_HOME` can select another. These files contain credentials and messages, so preserve owner-only access.
5. Run `ax agents` to start the broker from the installed binary, then relaunch the saved AX names.

Saved conversations and mail are preserved. AX 0.8.0 upgrades the mailbox to schema 4. A 0.7.x broker cannot open it. Reinstalling an older binary is not a rollback; preserve the backup and any messages written after the upgrade. Existing bridges can reconnect for local messaging, but relaunch sessions after active work finishes to load the new tools and cross-machine guidance.

In AX 0.7.0, replace the old `-name` option in saved commands with `-n`, for example `ax claude -n api`. Keep the same agent name to retain its identity and saved conversation.

## Optional installation settings

Download the installer first to inspect it or select a release:

```sh
curl -fsSL https://useax.dev/install.sh -o install-ax.sh
AX_VERSION=v0.8.2 sh install-ax.sh
```

`AX_INSTALL_DIR` chooses another binary directory. `AX_NO_MODIFY_PATH=1` disables changes to shell configuration. Set these variables on the `sh install-ax.sh` invocation. With a custom directory, add that directory to PATH before running AX.

Archives, checksums, and the MIT license are also available from [GitHub Releases](https://github.com/summationai/agent-exchange/releases/latest).

## Optional Tailscale messaging

Local-only operation remains the default. Install and connect Tailscale separately on both machines, then follow [Tailscale setup](https://useax.dev/docs/tailscale) only when the user requests cross-machine messaging. Pairing cards identify AX installations; discovery and messaging require explicit grants. Run `ax network serve` on each paired machine. The installer does not change Tailscale settings, pair machines, or enable a listener.

## Remove AX

When the user requests uninstallation, remove the installed `ax` binary and any installer-added PATH line that is no longer needed. The default binary is `$HOME/.local/bin/ax`. Preserve `$HOME/.ax`, which contains saved identities and mail, unless the user also asks to delete that state.

## Further details

- [Pi setup and resume](https://github.com/summationai/agent-exchange/blob/main/claude.rc/pi.md)
- [Usage, permissions, and resume](https://github.com/summationai/agent-exchange/blob/main/claude.rc/README.md)
- [Tested platforms and harness versions](https://github.com/summationai/agent-exchange/blob/main/claude.rc/verification.md)
- [Adding a coding harness](https://github.com/summationai/agent-exchange/blob/main/claude.rc/adapters.md)
