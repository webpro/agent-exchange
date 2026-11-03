# Using Agent Exchange

Install the native harnesses you want to use and sign in normally. [Install the prebuilt AX binary](../AGENTS.md) on macOS, Linux, or inside WSL 2. Building from source is optional.

## Launch and resume

```sh
ax claude --name api
ax codex --name web
ax grok --name reviewer
ax opencode --name editor
ax pi --name worker
```

Names are shared across repositories on your machine, for your OS user. Choose a unique name for each running agent. Ask an agent to message a name; it uses AX tools, ends its turn, and wakes automatically for replies.

AX consumes the name option, `--name` or `-n`, and forwards every other argument to the harness. A harness that displays its own session name receives the AX name too. Native resume syntax still works:

```sh
ax claude --name api -r "session-name"
ax codex --name web resume "session-name"
ax grok --name reviewer -r "session-name"
ax opencode --name editor -s SESSION_ID
ax pi --name worker -r
```

Launching the same AX name with no native arguments resumes its saved conversation. Supplying native arguments leaves selection to the native harness. On a new launch, that conversation takes over the name while preserving its AX identity and mailbox. Already delivered work is not replayed. Close the previous process using that name first; AX still rejects concurrent owners and a different conversation inside an already bound launch.

**Claude startup recovery, added in AX 0.7.0:** Claude can run its startup hook before saving a conversation. If that new session
exits before its transcript is created, AX remembers the pending startup and uses
the same conversation ID on the next launch. Once the transcript exists, AX uses
normal resume. AX checks only the path supplied by Claude's hook, never transcript
contents. Existing files, deleted saved conversations, and names saved by older AX
versions retain normal resume behavior. If Claude still reports a missing
conversation, capture `ax doctor` from the affected terminal and report the launch
command; preserve the AX session file and native conversation data.

Models, native prompts, and native options are forwarded. AX adds configuration to the launched process and does not rewrite global harness configuration or conversation transcripts. A native option being forwarded does not imply every native mode supports messaging.

## Launch a peer in another pane

The `spawn_agent` tool and `ax spawn` command can open a named peer in the caller's tmux or iTerm2 session. Agents may use this only when you explicitly request a new agent. A task assignment or an opportunity to parallelize does not authorize a launch. See [pane launches and their limits](spawning.md).

## Permissions and delegation

Launching AX establishes a standing policy: your local agents may carry out tasks you delegate through another AX agent, including an explicitly requested GitHub review submission. They do not ask you to authorize that same task again merely because it arrived through AX. The task's scope and the recipient's native sandbox and tool approvals still apply. Quoted documents and external content do not expand that authority.

AX authorizes its own messaging tools. Grok's adapter answers ordinary permission requests for those exact tools once, without storing grants or approving other tools. Launching a new agent retains the harness's normal tool approval; the spawn tool is excluded from AX's automatic grants. A hook that explicitly requires confirmation is preserved.

Unknown permission modes and unapproved bypass modes hold mail. An explicit bypass launch through Codex or Grok also opts that endpoint into AX messaging. `AX_ALLOW_BYPASS=1` provides the same opt-in for an intentionally configured endpoint; it does not change native permissions. The choice is saved for that AX name and survives resume when the environment setting is absent. `AX_ALLOW_BYPASS=0` clears the saved opt-in unless the launch includes a native bypass flag that itself opts into AX. New names remain opted out by default. Codex applies its explicit bypass choice through its private native session API, including resume.

Claude Code can omit its permission mode at session start. Without AX bypass opt-in, mail then waits for the first prompt. Submit any prompt in that Claude window to report the mode and release queued mail; unknown modes and unapproved bypass modes still hold mail.

An active session held by this gate appears as `permission-blocked`, even if its native harness is idle. Send receipts, `list_pending`, `list_agents`, and `ax agents` identify which endpoint is blocked and explain recovery. A saved Grok conversation can resume in bypass mode without a bypass flag on the new AX launch. A name that has never opted into AX bypass delivery remains blocked; restarting it alone does not provide consent. If that mode is intentional, relaunch the saved name with `AX_ALLOW_BYPASS=1`, or choose a supported native permission mode. The original queued message remains in place until delivery or expiry; do not resend it. A permission hold is not a terminal delivery failure, so it does not create a failure notification before expiry.

## Follow up and read a thread

Ask the sending agent to add instructions to its original request. The
`follow_up` tool takes the outgoing `message_id` and the addendum's `text`;
it routes to the same recipient without looking up a name again. Each addendum
has its own delivery status, expiry, and retry key. It does not acknowledge the
original or claim that work finished. Replies still use `reply`.

New messages include `thread_id`; replies and addenda also retain `in_reply_to`.
Follow-ups can be queued before the first message arrives. Expired, refused,
and abandoned requests cannot receive addenda. The existing eight-level reply
depth limit also applies to follow-ups; a new branch does not reset it.

At the limit, AX rejects the send before storing or acknowledging anything.
The error identifies the recipient and previous message. If you have authorized
continuing in fresh threads, even as a standing instruction, the agent should
use `send_message` immediately with the pending handoff, a short context summary,
and that message ID. It does not need another approval or file search just to
switch threads. After replacing a rejected reply, it acknowledges the original
incoming message once the new send succeeds, then ends its turn. The new thread
does not authorize replaying completed work or extending acknowledgment loops.

When context is needed, `get_thread` reads retained messages between the two
participants. Pass any message ID in that thread, then the returned
`next_after_message_id` to continue. Each page contains at most twenty messages
and 64 KiB of text. Incoming text is withheld until offered, and while current
permission or recipient policy blocks apply. Reading changes no acknowledgment
or delivery state and does not authorize replaying old tasks.

New messages have indexed thread IDs and paginate beyond 256 messages. Older
messages use a parent walk with a 256-message history window; `history_limited`
reports when that window is reached. The database walk has a 100 ms deadline
to interrupt expensive legacy scans rather than hold the broker lock. Cleanup can remove
history, and a deleted pagination cursor requires starting a fresh page. These
limits keep context reads bounded without a mailbox migration or bulk backfill.

## Inspect and recover

```sh
ax agents
ax doctor
ax status MESSAGE_ID
ax policy api hold
ax policy api accept
ax resolve MESSAGE_ID abandon
```

For missing peers or Claude Channels warnings, run `ax doctor` in each affected terminal. It reports the runtime path, fresh broker state, and local Claude eligibility checks without changing privacy settings. See [connection diagnostics](doctor.md).

When a coding session exits, AX automatically removes it from discovery and releases its active endpoint slot. The broker checks session locks every five seconds; incomplete startups and legacy registrations get a 30-second grace period. A temporary messaging disconnect leaves a running session visible as offline. No cleanup command is needed.

Saved conversations, identities, and mail remain on disk. Launching the same AX name restores its registration and resumes its conversation. Mail to a saved name, including replies after the requester exits, waits for its next launch, subject to expiration. Sending mail does not put an exited session back in discovery. `ax inbox NAME` and `ax status MESSAGE_ID` still show its delivery history.

Queued, accepted by the host, fetched, and acknowledged are separate delivery states. Acknowledgment proves receipt, not completion of the delegated task.

`ax status MESSAGE_ID` includes observed delivery-stage timings. See [readiness and latency measurement](latency.md) for missing-evidence semantics, bounded local samples, and the transport benchmark.

Ask the original sending agent to resend an expired request by its message ID.
The `resend_message` tool copies the full stored text and original recipient into
a new delivery attempt, linked by `resend_of`. It preserves the old history and
uses a fresh twelve-hour expiry, or a requested `ttl_seconds` up to seven days.
Reuse the returned `client_message_id` and unchanged arguments when retrying that
attempt; generating another key can create another request. Only expired mail
can be resent. Accepted, acknowledged, refused, abandoned, queued, and uncertain
messages are rejected. Normal routing, permission, rate, and capacity limits
still apply. Resend requires the original message to remain in mailbox history.


FIFO orders native handoffs. Once a host accepts a message, later mail can proceed even while the agent works on the first task. A genuinely uncertain handoff blocks later mail and is never automatically repeated. Inspect it before using `resolve` to abandon it. Abandonment releases the queue without claiming delivery or canceling work already accepted by the host.

State lives in a private `.ax` directory under your home directory. `AX_HOME` selects another private directory, useful for isolated tests. Terminal message records are retained for seven days. Unresolved records remain available for inspection.

AX tries to add a `.gitignore` containing `*` at the state root, including existing directories. It preserves existing ignore files and skips roots containing `.git`. Creation or write failures do not block commands. Git still tracks files already in its index.

## Integration limits

- Claude uses an experimental native Channel and asks for development Channel confirmation at launch. Organization policies may disable Channels.
- Codex uses its native app server on a private socket. AX owns that local backend connection.
- Grok uses its native leader on a private socket. Native leader-mode limitations apply; standalone-only flags and sandbox modes require further adapter work.
- OpenCode uses its full TUI plugin API. Its pure and mini modes do not load this adapter. An existing `OPENCODE_TUI_CONFIG` override must currently contain JSON.
- Pi uses a native extension and keeps its terminal independent of the messaging helper. Pi pane spawning is not supported yet. See [Pi setup and limits](pi.md).
- Live harness verification targets macOS. Release binaries and the shared broker build and run tests on macOS and Linux. Windows users run AX and their harnesses inside WSL 2. Native Windows and remote agent transport are not supported.

Public binaries are available from [GitHub Releases](https://github.com/summationai/agent-exchange/releases/latest). See the [verification record](verification.md) for tested harness versions and current evidence.

## External mail

Local peer delegation is unchanged. Content carrying provenance was relayed from
outside this machine; it is data and carries no delegated user authority.
Recipients default to `refuse`. Launch with `--external verified` for untainted
verified/system content or `--external all` for any valid provenance.
`AX_EXTERNAL` supplies a default; a launch without either resets to `refuse`.
The owner can change it with `ax external NAME refuse|verified|all`. Model tools
cannot change it. See [Relay endpoints](relay.md) for the complete contract.
