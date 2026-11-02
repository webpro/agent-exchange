# Verification

Preview verification, September 19, 2026. Live host tests run on macOS arm64 in an isolated AX state directory.

| Harness | Version | Native integration |
| --- | --- | --- |
| Claude Code | 2.1.278 | MCP and native development Channel |
| Codex CLI | 0.154.0 | MCP and private native app server |
| Grok Build | 1.0.34 | MCP and private native leader |
| OpenCode | 1.18.31 | MCP and per-launch TUI plugin |

## Codex turn observation

November 3, 2026. Codex CLI 0.166.0 in a real TUI with an isolated mock provider reported busy during a turn and ready after it failed. With the broker suspended, the launcher exited 1.1 seconds after `/quit` was submitted. Idle observation left the session file unchanged over 11 seconds, and broker timeouts produced no lifecycle errors in the TUI.

The interruption-notice check queued three distinct messages, then interrupted four turns. The sender received three notices, one per message and offer attempt. All three wakes remained queued 20 seconds later. A unit check also verified that a new offer attempt can produce a new notice after the earlier notice was acknowledged.

## Live evidence

Grok and OpenCode automatically connected, discovered each other, and completed a round trip containing `AX_GROK_OPENCODE_OK`. Both records reached `acknowledged`. The same named conversations were closed and resumed with unchanged native IDs; queued mail then reached the resumed recipient without replaying accepted messages.

Codex and Grok completed another round trip containing `AX_CODEX_GROK_OK`, with both messages acknowledged. Codex used its native read-only mode and Grok used its default permission mode. Grok's AX tools worked without approving unrelated tools or changing global permissions.

OpenCode and Claude completed a round trip containing `AX_CLAUDE_OPENCODE_OK`, with both messages acknowledged. Grok then sent two separate requests to OpenCode without waiting; both replies returned and all four records were acknowledged. Ten messages across these tests reached acknowledgment. Recent native handoffs completed in 0–24 milliseconds; model responses took additional seconds.

OpenCode's idle listener remained connected beyond the native fetch client's default five-minute timeout after the adapter disabled that timeout. Its later cross-harness reply still arrived automatically.

All four hosts reconnected after restarting only the isolated broker. OpenCode and Grok then completed another round trip containing `AX_FINAL_RESTART_OK`, bringing the total to twelve acknowledged messages. A resumed OpenCode session also loaded an existing relative plugin from nested TUI configuration alongside AX.

The preceding Claude/Codex implementation has live evidence for two-way delivery, cross-directory discovery, adoption of existing conversations, immutable bindings, broker restart, scoped delegation, and explicit Codex bypass resume. The new preview keeps those paths and their regression coverage.

## Broker repair

An overnight log inspection found later messages blocked behind a request Codex had already fetched but never acknowledged. FIFO now advances after confirmed native handoff or content fetch, independently of task completion. The live broker released the waiting messages and preserved the fetched request without replay. Recovery also recognizes stored confirmation receipts that older brokers downgraded during disconnect.

## Automated checks

The race-enabled Go suite and vet pass locally. Focused tests cover immutable adapter binding, native configuration passthrough, Grok queue acceptance versus socket write, the six-tool permission boundary, confirmed handoff recovery, and genuinely uncertain delivery barriers. CI runs the Go suite, vet, and build on macOS and Linux.

The initial repository scope passed autoreview after fixing preservation of nested OpenCode TUI plugins and their relative paths.

## Binary installation

[AX 0.5.3](https://github.com/summationai/agent-exchange/releases/tag/v0.5.3) publishes macOS and Linux archives for x86_64 and ARM64. All four native builds passed the race suite, vet, and installation tests that launch the installed broker. Linux archives contain statically linked binaries.

The same installer tests passed inside Ubuntu on Windows WSL 2, using the Linux x86_64 release binary. The tests exercise installation, repeat installation, shell PATH preservation, a real broker connection, failed downloads and checksums, custom install directories, and symlink targets. See the [release workflow results](https://github.com/summationai/agent-exchange/actions/runs/35471875199).

The authenticated installation command documented for the private 0.5.3 preview also downloaded and installed the published archive on macOS ARM64. The installed binary reported `0.5.3`, and doctor found all four native harnesses. Installer and release changes passed autoreview, including the Windows path and shell line-ending fixes found by live CI.

## Public installation

The repository is public as of September 19, 2026. [AX 0.5.4](https://github.com/summationai/agent-exchange/releases/tag/v0.5.4) downloads with curl and requires no GitHub account or GitHub CLI. All four native builds and the WSL 2 installation checks passed in the [release workflow](https://github.com/summationai/agent-exchange/actions/runs/35472738163). The installer suite now also checks explicit version selection and rejects malformed tags and unexpected latest-release redirects. Autoreview reported no actionable findings.

The exact public README command installed and reinstalled 0.5.4 on macOS ARM64 in a fresh home directory with no GitHub credentials. It preserved a single shell PATH entry, and the installed broker started and answered an agent-list request. The published installer matched both its release checksum and the reviewed source.

For that release, live harness exchanges on Linux and WSL, native Windows, remote transport, every native subcommand, and all host policy configurations were outside the verified scope. Native acceptance is distinct from a model acknowledgment and from successful task completion.

## Pi

September 21, 2026. The adapter included in AX 0.6.1 was tested with Pi 0.86.1 and Codex CLI 0.154.0 on macOS ARM64, using a separate AX broker, temporary state, and headless native processes. Existing user sessions and the installed AX binary were unchanged.

Pi loaded the real extension and its TypeBox schemas, registered the shared AX tools, and became ready with zero model messages. Pi sent `AX_PI_TO_CODEX_OK`; Codex read it through AX and replied `AX_CODEX_TO_PI_OK`. Pi woke automatically and acknowledged the reply. Both mailbox records reached `acknowledged`.

Killing only Pi’s messaging child left the native Pi process alive. The extension reconnected with the same conversation ID. Restarting only the isolated broker also recovered without restarting Pi. Closing and relaunching Pi with its saved session file from another repository preserved the native ID. A native new-session command changed Pi’s conversation, left the original AX binding intact, and disconnected that name.

Contract tests additionally cover busy follow-up delivery, native argument preservation, immutable session binding, reloads during an outstanding initialization, new/fork lifecycle events, rejection of stale connection results, and MCP initialization. Native process tests verify that AX is replaced by Pi, the name lock lasts until Pi exits, and Pi’s exit status is preserved. Pi pane-spawn requests are rejected before opening a pane or reserving a name. The JavaScript contract fixture stubs schema construction; the live exchange loaded the actual schemas.

Pi’s live evidence uses RPC mode. Interactive TUI rendering, live busy-turn delivery, and Linux/WSL exchanges remain outside the verified scope. A pipe-write receipt remains distinct from confirmed native acceptance or a model acknowledgment.

## AX 0.8.0 and Tailscale

October 15, 2026. Source `fd068ab60d427ffd910d72059582c18a4f3f3b38` combines the reviewed Tailscale stack at `2891c4c` with the three fixes shipped in 0.7.2. The two-host tests used separate AX state on two physical Apple Silicon Macs. Native Codex was 0.162.1; the existing local Grok reviewer was 1.0.52.

A laptop MCP test endpoint sent a request to native Codex on the second Mac. Codex fetched it through its AX tool, replied with the expected token, and independently sent a new message back. Qualified remote names and the source reply mapping were verified. The native session continued in tmux after its launching SSH connection ended.

A second request was submitted while only the destination network adapter was stopped. Its broker and Codex session survived. After adapter restart, the request reached Codex and the expected reply returned without restarting the native session. The sender recorded acknowledgment; remote task completion remained unknown.

The user's normal local AX runtime then exchanged a request and reply with the remote Codex worker. An existing native Grok session also sent a request to that Codex worker, received its reply, and returned the expected token to the initiating endpoint. That complete model-driven route took 61.254 seconds; it is not a transport-latency measurement. Subsequent delegated work ran in another remote Codex conversation in a real repository.

Idle snapshots of the two persistent broker/adapter pairs showed 0.0 to 0.4 percent CPU and about 15 to 23 MiB RSS per helper. Earlier isolated tests exercised peer rejection, revocation, bounded discovery overflow, and adapter shutdown while local MCP stayed usable. These are bounded observations, not a long-duration resource guarantee or a live suspend/resume test.

Before upgrading the user's normal runtime, a consistent schema-3 mailbox copy migrated to schema 4 with all 951 message payloads and 22 agent credential hashes unchanged. At the later cutover, all 963 original messages and 22 credentials were retained. Two running 0.7.2 Claude-channel bridge fixtures also exchanged messages before and after a broker-only upgrade, retaining their bridge PIDs. This tests bridge compatibility; it does not claim a native Claude model exchange across two hosts.

The full race suite, vet, and build passed on the combined source. Autoreview and the AX peer review were clean. Release-version packaging and the accompanying website are checked separately at the release head. Live cross-machine Linux/WSL, address changes, suspend/resume, and a two-host native Claude-to-Codex exchange remain unverified. Certificates require explicit manual re-pairing for rotation; remote spawning and automatic task-outcome synchronization are not provided by the network adapter.
