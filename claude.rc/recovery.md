# Messaging recovery

Tracks [issue #4](https://github.com/summationai/agent-exchange/issues/4).

Each MCP tool call has a ten-second budget for obtaining a broker connection,
binding the session, confirming readiness and executing its operation. An attempt
gets at most four seconds within that shared budget. After a transport failure,
the bridge retires that connection and can try once more through the existing
reconnect loop. Waiting for a replacement uses a notification, not a polling loop.
Broker rejections are returned without automatic retry.

Send and reply keep the same `client_message_id` across both attempts. Success
and failure results expose the key so a later tool call can reuse it. Retrying
with the same key and unchanged arguments returns the original durable message.

Failures include structured fields in addition to their JSON text representation:

- `retryable`: whether recovering the transport or waiting for resource cooldown
  or the send rate limit can make a later attempt useful.
- `submission`: `not_submitted`, `outcome_unknown`, or `not_applicable` for reads.
- `client_message_id`: the key to preserve when retrying a send or reply.
- `connection_state`: `reconnecting` for transport failures.
- `retry_after_ms`: the remaining resource cooldown or rate-limit window, when applicable.
- `recovery`: the next step, such as the change to make or the key to reuse.

`outcome_unknown` means the request may have been stored despite the missing
response. It does not mean delivery failed. A broker error after submission is
also treated conservatively because it can report a commit failure.

A send, reply, resend or follow-up that the broker refuses before storing anything, such as one to an unknown target, is the exception: it reports `code: request_refused` and `submission: not_submitted`, and its `recovery` says what to change. Only a rate-limited send is `retryable`, after `retry_after_ms`. When an earlier attempt of the same call lost its response, a refused retry stays `outcome_unknown`.

## Recover a missed wake

Ask the agent to call `list_pending` once after a reconnect or when a request
seems missing. It lists only that agent's unacknowledged incoming mail, ordered by
recipient sequence. Each page contains at most 50 entries with message ID, sender,
age, expiry, delivery state, and a preview of up to 100 Unicode characters. When
`next_after_seq` is present, pass it as `after_seq` to read the next page. These are
live pages: acknowledgments can remove entries, and new mail has later sequences.

Listing changes no receipts and never replays a task. Queued previews remain
hidden until the message has been offered; hold, refuse, and unapproved permission
modes also hide previews. Offered IDs can be fetched through `get_message` during
recovery. An uncertain handoff remains uncertain: check whether work was already
started before acting. Later queued messages identify the earlier FIFO blocker.
Terminal messages are excluded. Neither discovery nor a fetched preview means the
user requested that work to be executed again. Never poll this tool for replies.

## Read a send receipt

Successful `send_message` and `reply` results include `message.receipt` with the
stable `client_message_id`, recipient readiness, snapshot time, queued count,
unacknowledged count, and delivery evidence. The message itself includes its
effective `expires_at_ms`. Readiness is a snapshot, not a guarantee. Offline and
starting recipients retain queued mail; the receipt says why it is waiting.

`wake_accepted` means the harness accepted a wake. `channel_written` means AX wrote
to its channel. `content_served` means the recipient fetched the body. Only
`acknowledged` records an acknowledgment or reply, and none of these states proves
task completion. Send receipts and `delivery_status` report `task_completion` as
`unknown`. Retrying the same request returns the same message and expiry, with an
updated readiness and delivery snapshot.

`delivery_status` includes `message.superseded_by` when a live replacement exists, including after acknowledgment. The field is computed when read; it does not change the recorded delivery state or prove which task the recipient performed.

## Choose an expiry

Both send and reply accept the optional integer `ttl_seconds`. Omitted values
default to 43,200 seconds (12 hours). Values must be between 1 and 604,800 seconds
(7 days), so a request can explicitly wait through a weekend. Nulls, strings,
fractions, zero, negative values, and values above that limit are rejected before
mail is queued. AX does not infer an expiry from the message's wording.

Keep the same expiry when retrying an idempotency key; changing the effective TTL
is a conflict. TTL controls queued delivery, not a running task's deadline. Once
handoff has started, expiry does not cancel or automatically repeat uncertain or
accepted work. Expired queued requests produce a [sender notification](delivery-notifications.md).

Longer TTLs share the existing 1,024-message and 32 MiB unresolved-mail capacity
limits. Unresolved messages survive cleanup; terminal records become eligible for
cleanup seven days after creation. No mailbox migration is needed. Install an
updated broker and relaunch messaging integrations through the normal update
procedure to expose these tools; installing a binary does not replace live processes.
