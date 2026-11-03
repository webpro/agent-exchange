# Adding a harness

For an existing local runtime that can start an MCP child, use [local attachment](attach.md). It shares AX's tools without requiring a compiled launcher. A runtime can receive mail on a user turn or provide a private native wake socket. Installable adapter packages and scaffold commands are planned, not implemented.

The source contribution path below adds a built-in launcher and requires a rebuild.

The useful native capabilities are: configure AX's MCP tools for this launch, identify the conversation selected by the user, report its state and permissions, and wake that exact conversation with confirmed acceptance. MCP tool support alone does not supply the native wake integration.

Add one entry to `harnesses` in `internal/ax/adapters.go`, plus a preparation function for the native harness. The broker does not need a new implementation or a host-specific message format.

An adapter supplies the native tool-name prefix, session-ID validator, and a preparation function. That function receives the original native arguments and environment and returns the launch arguments, environment, cleanup function, or an actionable error. Add the command to CLI dispatch and doctor output.

If the harness accepts a session display-name option, set `nameFlag` in its registry entry. AX passes its own name through only when the installed CLI advertises that exact option. The help probe has a three-second timeout; missing or unsupported options leave the launch working. Claude and Pi use this path. An option that selects an agent definition is not a session display name.

Use a native API, lifecycle hook, or trusted plugin to identify the conversation the user selected. `bindAdapter` rejects rebinding an AX name to a different conversation. Do not discover identity by searching transcripts or observing terminal text.

Context observations use `ax.lifecycle.context_usage`: optional `context_used_tokens` and `context_window_tokens`, with `context_observed_at_ms`. A newer snapshot replaces missing counts too. Codex reports `last.totalTokens` and `modelContextWindow` from the selected thread's `thread/tokenUsage/updated`; cumulative totals are not context usage. Unsupported harnesses omit the counts.

For a Go integration, `startAdapterHost` accepts a native wake callback. Return success only when the host confirms acceptance. Grok demonstrates this with ACP prompt IDs and native queue events; a socket write alone is insufficient.

For a native plugin, the same private per-launch Unix socket exposes:

| Operation | Contract |
| --- | --- |
| `POST /bind` | Report selected `native`, native `permission`, and `state` from the harness API. |
| `GET /next` | Wait for a fixed wake containing `id`, `native`, and `text`. |
| `POST /receipt` | Confirm the wake by `id`, or report an `error`. |

OpenCode's embedded JavaScript plugin demonstrates that path. Keep its long-lived connection alive without depending on a model turn. Verify the recipient's native ID before invoking the host API. Keep peer bodies in the shared MCP tools.

The model-facing tools are `list_agents`, `send_message`, `reply`, `get_message`, `ack_message`, and `delivery_status`. Reuse `ax bridge`; it filters model-supplied arguments and prevents forged identity, permissions, receipts, and policy changes.

The Pi adapter uses a different lifetime boundary: AX replaces itself with the native CLI, and the extension owns an optional `ax bridge` child over standard input and output. The bridge still connects to the shared Unix-socket broker. Full peer bodies arrive as Pi custom messages, and `channel_written` records only a pipe write. A model reply or acknowledgment is separate proof of receipt. Messaging failures must leave the native CLI running. See [Pi setup and verification](pi.md).

Prove these behaviors before enabling an adapter: fresh launch and native resume, automatic readiness, unchanged prompts and settings, two-way delivery with another harness, native permission preservation, busy-session delivery, immutable identity, disconnect cleanup, and ambiguous handoff handling. Include a protocol-level test that rejects a false acceptance signal. Record the exact native version and live evidence in the verification document.

Use `ax verify NAME` to check one complete request and reply. Discovery reports tools and wake capabilities separately; configuration and host acceptance are not proof of a completed exchange.

## Contribution workflow

1. Inspect the harness's native API and record which capabilities and versions it supports. Use the user's existing conversation and native argument syntax.
2. Implement the preparation function and registry entry. OpenCode is the reference for a native plugin; Grok is the reference for a native server. Claude and Codex still have specialized launcher paths.
3. Add CLI dispatch and doctor support, focused protocol tests, and an isolated live exchange with an existing supported harness.
4. Run the checks below and update the support table and verification record with the evidence. Keep host limitations explicit.

```sh
go test -race ./...
go vet ./...
```

Keep one messaging core. An adapter should not implement its own broker, mailbox, retry policy, or model-facing messaging tools.
