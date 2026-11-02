# Supported harnesses

AX connects Claude Code, Codex CLI, Grok Build, OpenCode, and Pi. Each integration loads the shared messaging tools and uses the harness’s native APIs to identify and wake the selected conversation.

## Launch commands

| Harness | Start a named session | Native integration |
| --- | --- | --- |
| Claude Code | `ax claude --name api` | MCP tools and a development Channel |
| Codex CLI | `ax codex --name web` | MCP tools and a private native app server |
| Grok Build | `ax grok --name reviewer` | MCP tools and a private native leader |
| OpenCode | `ax opencode --name editor` | MCP tools and a per-launch TUI plugin |
| Pi | `ax pi --name worker` | Native extension and an optional messaging child |

Names work across repositories for the same OS user. AX consumes the name option and passes other arguments to the native harness. [Resume commands](README.md#launch-and-resume) work with existing conversations.

A harness that names its own sessions also receives the AX name, so the agent is identifiable in its native interface. Claude Code is the only one today: the name appears in its prompt box, `/resume` picker, and terminal title. A Claude release without that option still launches without a native name.

## Integration limits

Claude uses an experimental Channel and requires development Channel confirmation at launch. Organization policy can disable Channels.

Codex requires native queue support. AX owns the private app-server connection used by this integration; complete crash isolation for that backend remains pending.

Codex sessions that expose native thread state advertise `busy` and `idle` lifecycle observation. Busy sessions continue accepting FIFO wakes. A lost observed connection holds mail; `systemError` remains ready because the backend accepts turns. Older backends without state events keep the conservative lifecycle contract.

Grok runs through its native leader. Leader-mode restrictions apply, and standalone-only flags and sandbox modes need further adapter work. Complete backend crash isolation is also pending for Grok.

OpenCode uses the full TUI plugin API. Its pure and mini modes do not load AX’s adapter. An existing `OPENCODE_TUI_CONFIG` override must currently contain JSON.

Pi uses its native extension API. Messaging failures leave Pi running. Starting a new conversation or fork disables messaging for the original AX name; use a new name for the new conversation. Pi 0.86.1 is tested. See [Pi setup and limits](pi.md).

Forwarding an argument does not guarantee that every native mode supports messaging.

## Platforms and tested versions

Prebuilt AX binaries support macOS 13 or later and Linux, on ARM64 and x86_64. Windows uses WSL 2; AX and the harnesses must run in the same distribution.

All five harnesses have completed live exchanges on macOS. Pi’s live checks used its RPC mode; interactive TUI rendering remains unverified. Binary and broker checks cover macOS, Linux, and installation inside WSL 2. Live harness exchanges on Linux and WSL remain unverified. The [verification record](verification.md) lists exact versions and evidence.

## Another harness?

Adapters currently require a source contribution and rebuild. AX does not yet offer installable adapter packages or a scaffolding command.

The [adapter guide](adapters.md) explains the native capabilities, lifecycle contract, and checks needed to add one. A new adapter reuses the broker, mailbox, and messaging tools.
