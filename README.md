# Agent Exchange

Give your coding agents names. Let them talk to each other.

Agent Exchange (`ax`) connects Claude Code, Codex CLI, Grok Build, OpenCode, and Pi on your machine. Each agent keeps its harness's terminal UI, model, permissions, and existing conversations. Optional [Tailscale pairing](claude.rc/tailscale.md) connects agents on your other machines.

## Install

Ask your coding agent:

> Read https://useax.dev/agents.md and install Agent Exchange.

You can also follow [AGENTS.md](AGENTS.md) yourself or browse the [documentation](https://useax.dev/docs). The guide covers installation, verification, updates, and removal. AX uses prebuilt binaries, so you do not need Go, Make, or a GitHub account.

- **macOS 13 or later:** native binary for Apple Silicon or Intel.
- **Linux:** static binary for ARM64 or x86_64.
- **Windows:** use WSL 2, with AX and the coding harnesses in the same Linux distribution.

Install and sign in to your coding harnesses separately.

AX 0.8.2 starts recognized option-only Claude pane launches with one AX setup turn before delivering queued tasks. It retains reusable agent names, queued-message abandonment, and opt-in Tailscale messaging. See the [release notes](claude.rc/release-notes.md). The installer downloads the latest published release.

## Try it

Open two terminals, in any repositories:

```sh
ax claude --name api
ax codex --name web
```

In Claude's window, submit any prompt first so AX can learn its permission mode and release held mail.

Ask Codex: **“Ask api whether the schema is ready.”** Claude receives the request and can reply into the same Codex conversation. Both agents connect automatically. Names work across repositories on the same machine for the same OS user.

Open `ax inbox` in a third terminal or a terminal split to watch messages separately. Use `ax inbox api` to filter messages to and from one agent. The read-only view shows delivery status and message bodies; it does not acknowledge messages or control agent sessions. [Inbox controls and behavior](claude.rc/inbox.md).

Grok, OpenCode, and Pi join the same exchange:

```sh
ax grok --name reviewer
ax opencode --name editor
ax pi --name worker
```

## Across machines

Pair two AX installations through Tailscale, explicitly grant messaging, and address a remote agent as `reviewer@buildbox`. Each machine keeps its own broker and mailbox. Messages wait on the sender while the other machine is unavailable, subject to expiry.

Local messaging works without Tailscale. Updating AX does not pair machines or start a network listener. See [Tailscale setup](claude.rc/tailscale.md).

## Resume a conversation

AX reads the name option and passes other arguments to the native harness. Use the harness's resume syntax to continue an existing conversation:

```sh
ax claude --name api -r "session-name"
ax codex --name web resume "session-name"
```

Launching the same AX name with no additional arguments resumes its saved conversation. See [usage and resume](claude.rc/README.md) for details.

## Supported coding harnesses

- **Claude Code:** MCP tools and a native Channel. Requires development Channel confirmation at launch.
- **Codex CLI:** MCP tools and a private native app server. Requires native queue support.
- **Grok Build:** MCP tools and a private native leader. Native leader-mode limitations apply.
- **OpenCode:** MCP tools and a TUI plugin loaded for each launch. Supports the full TUI; pure and mini modes are unsupported.
- **Pi:** Native extension with automatic connection, idle wake, and busy follow-ups. Pi keeps running if its optional messaging helper fails. See [Pi setup and limits](claude.rc/pi.md).

All five have completed live message exchanges on macOS, including resumed conversations. Automated checks cover release binaries and the broker across platforms. Live harness exchanges on Linux and WSL remain unverified. See [tested versions and evidence](claude.rc/verification.md).

## Add another harness

A new adapter connects a harness's launch, session selection, and message-wake APIs to AX. It reuses the broker, durable mailbox, MCP tools, and delegation policy.

Start with the [adapter guide](claude.rc/adapters.md). Add a registry entry and a native preparation function, connect the command to CLI dispatch and `ax doctor`, then test launch, resume, delivery, and permissions. OpenCode is the example for a native plugin; Grok is the example for a native server.

Adapters currently require a source change and rebuild. Installable adapter packages, scaffolding, and a shared compatibility runner are planned but not yet available.

## How it works

One Go broker serves the local agents. SQLite keeps queued messages across restarts. Agents send through AX tools, finish their turn, and wake for replies. A queued message, a message accepted by the harness, and an acknowledged message are distinct delivery states.

Delegated tasks retain their scope and the receiving agent's native permission controls. AX configures each launched process without rewriting global harness configuration or conversation transcripts.

See the [architecture](claude.rc/architecture.md) for the broker and delivery model.

## Resource protection

The broker, bridges, lifecycle hooks, and inbox share a CPU budget. After an initial ten-second observation period, five CPU seconds across these helpers in a ten-second window pauses messaging for sixty seconds. Native coding sessions keep running during this pause; queued messages remain in SQLite.

Run `ax doctor` to see whether messaging is paused and when the cooldown ends. Bridges normally keep their MCP connection open and reconnect to the broker after cooldown. A standalone helper that continues consuming sustained CPU can terminate itself; its harness may then require the MCP connection to be reconnected.

This is a sampled circuit breaker, not a hard CPU quota. Native harnesses and AX launchers that own native backends are outside the budget. Complete crash isolation is still pending for the Codex and Grok native backend integrations. Database cleanup, diagnostic logs, and adapter connections also have bounded work, but AX does not enforce a hard memory or total database size limit. [Protection behavior and limits](claude.rc/resource-safety.md).

Updating the binary preserves saved conversations and mail. Already-running processes keep their old code, so new protections apply only after those helpers are replaced. Follow the [update guide](AGENTS.md); replacing the executable alone does not upgrade a running broker or bridge.

Licensed under [MIT](LICENSE). Commercial and closed-source use is allowed; retain the copyright and license notice.
