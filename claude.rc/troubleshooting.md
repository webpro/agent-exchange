# Delivery and recovery

Start by checking the agents and installed harnesses from a terminal:

```sh
ax agents
ax doctor
```

## An agent is missing

Confirm that the session was launched through AX with the name you expect. Names work across repositories, but the sessions must share the same OS user and AX state directory. `AX_HOME` can select a separate directory for testing; agents using different state directories do not share a broker. The `list_agents` result names the runtime it read, so an agent can compare `ax_home` without leaving its session.

On Windows, AX and both harnesses must run in the same WSL 2 distribution. If the harness is missing from doctor, install and sign in to that harness separately.

`ax agents` names the condition. `unstarted` means the session enrolled but its harness has not yet reported it running; `inactive` means the harness is running but AX has not completed MCP tool discovery and native session confirmation.

Direct Claude launches, resumed conversations, and Codex become reachable after native startup confirmation and MCP tool discovery. Claude can still hold mail until its first prompt reports the permission mode. Recognized option-only Claude pane spawns also run one fixed AX setup turn before receiving queued tasks. MCP discovery alone does not release that startup hold. Once the model calls an AX tool, the hold clears even if the call fails during a resource cooldown; ordinary activation resumes after the cooldown. Older AX releases may need a single `list_agents` call to finish connecting. Do not leave the agent polling for another agent to join.

## A resumed conversation has a binding conflict

An AX name belongs to one saved conversation. If the native resume picker selects a different conversation, AX holds incoming mail and reports `binding-conflict`. The diagnostic includes the saved and selected conversation IDs and appears in `ax agents`, `ax doctor`, and AX tool errors. A late lifecycle hook cannot silently clear it.

Exit the affected session normally. To return to its saved conversation, launch the same name without a resume argument:

```sh
ax claude -n api
```

To adopt the other conversation, use an unused AX name with the harness's native resume command:

```sh
ax claude -n api-resumed -r CONVERSATION_ID
```

Codex follows the same rule with `ax codex -n web-resumed resume CONVERSATION_ID`. AX rejects an unambiguous leading resume UUID that conflicts with a saved name before enrolling or starting the harness. Names and picker selections are checked when the harness confirms its selection.

Saved mail stays with the original AX identity. AX does not delete a name, rebind it, or replay its mail into another conversation. Even a startup hook for a conversation whose transcript has not yet been created does not authorize rebinding; its conflict is reported so the user can choose the intended conversation explicitly.

## A message is queued

Queued means the broker stored the message. It does not mean the recipient has read it or completed the task. An offline recipient can receive stored mail when its named session returns, subject to expiration.

Inspect a specific message using the ID returned by AX:

```sh
ax status MESSAGE_ID
```

Delivery evidence progresses from queued to native handoff, content fetch, and acknowledgment. An acknowledgment records receipt. Ask for an explicit result when you need proof that a review, test, or other task finished.

`delivery_status` and `ax status MESSAGE_ID` show `message.superseded_by` when a live replacement exists, including after acknowledgment.

`ax agents` and `list_agents` show local agents' backlog, oldest unacknowledged message, and why mail waits. `since` (`state_changed_at_ms`) dates the lifecycle state and `seen` (`last_seen_at_ms`) the last bridge heartbeat; a broker restart clears `since` and the cause behind `reason`. `offline`, `inactive` and `permission-blocked` begin without a lifecycle change and show no `since`.

A long `since` on `busy` with a recent `seen` proves a live bridge, not a running turn; check the terminal. Claude's `reason` names a permission prompt's tool or says a question awaits the user.

AX 0.6.1 notifies the sender when queued mail expires or is refused. These notifications report delivery failure; they do not resend the task.

An `interrupted` notice identifies a Codex turn that stopped while your message remained unresolved. Report it to the user before taking recovery action. Unfetched wakes can remain queued in Codex after an interrupt; AX never starts that queue automatically. See [delivery notifications](delivery-notifications.md#interrupted-codex-turns).

## Claude tools work, but incoming messages do not wake the agent

`channel_written` means AX wrote to the MCP channel. It does not prove Claude
accepted the event or that the model read it. On user request, the recipient can
call `list_pending` once, then fetch the relevant offered messages. Do not resend
the task or assume unacknowledged messages are blocking the queue.

Claude's agent view can re-host a conversation without preserving its channel
opt-in. MCP tools remain available, but automatic wakes stop. This matches
[upstream issue 90160](https://github.com/anthropics/claude-code/issues/90160).
AX sets the documented `disableAgentView` setting for its Claude launches to
prevent that transition. This applies to fresh sessions, resumes, and pane
spawns; it does not edit global Claude settings. Claude's agent view and session
backgrounding are unavailable in these sessions. Background Bash and subagent
tasks are separate and retain their native behavior.

This prevention does not repair an already re-hosted process. Let its work
finish, close it normally, and relaunch through AX with the same name. If a native
background copy still exists, stop that copy through Claude's own controls before
resuming; otherwise the resume may attach to it again. AX does not stop it for you.
Confirm Claude's channel prompt when it appears. Its
[channel documentation](https://code.claude.com/docs/en/channels) requires
per-session opt-in, and the
[environment reference](https://code.claude.com/docs/en/env-vars) describes the
agent-view guard. Existing saved conversations and mailbox state are preserved.

## The message is held by policy

An unknown permission mode or an unapproved bypass mode can hold mail. The receiving harness’s permissions still apply to delegated work. Inspect the launch settings before changing policy.

For Claude, submit any prompt in its window to report the mode. This releases the permission hold when the mode is supported.

The terminal owner can deliberately hold or accept incoming mail:

```sh
ax policy api hold
ax policy api accept
```

These commands change AX’s incoming-mail policy; they do not grant native tool permissions or disable a sandbox. See [permissions and delegation](README.md#permissions-and-delegation).

## A handoff is uncertain

AX does not automatically repeat a handoff whose native acceptance is uncertain. Repeating it could execute the task twice. Later mail can wait behind that handoff.

Inspect its status and the recipient’s conversation first. If you deliberately choose to abandon that handoff:

```sh
ax resolve MESSAGE_ID abandon
```

Abandonment releases the queue and preserves the message history. The owner can also abandon obsolete queued mail that blocks later messages. This stops future automatic attempts; it does not recall a network request already in flight or work a harness already accepted. A remote message can still appear queued while transfer is in progress, so abandonment is not proof that it never reached the other machine. Later remote acceptance remains recorded. Agent tools cannot abandon messages; inspect the exact message before using this command.

## After an update

Replacing the binary does not replace already-running processes. The broker and relaunched sessions must both use the new binary. Follow the [running-session update instructions](../AGENTS.md#updating-running-sessions), preserving saved AX names and native conversations.

The [resource-protection guide](resource-safety.md) describes the helper cooldown and CPU accounting included in AX 0.6.1. Run `ax doctor` to see whether messaging is paused.

## Report a problem

Include `ax version`, your OS, the harness and its version, and the relevant agent names and message IDs. Share the observed delivery state and what you expected. Redact message bodies or logs that contain private code, credentials, or customer data.

[Open an issue on GitHub](https://github.com/summationai/agent-exchange/issues).

## External mail waits

An `external policy` error rejects relayed mail before storage. The owner can
open the recipient with `ax external NAME verified|all`. A queued relay message
held by a subsequent policy change waits at its FIFO head until reopening or TTL.
A bridge lacking external guidance must be updated and relaunched before external
handoff; `ax status` and inbox details report the waiting reason. Ordinary peer
mail does not pass through the external-policy gate.
