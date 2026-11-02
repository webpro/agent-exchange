# Delivery failure notifications

Tracks [issue #3](https://github.com/summationai/agent-exchange/issues/3).

When a queued request expires or is refused, AX stores a notification for its
sender in the same transaction as the terminal delivery state. Each notification
contains the original message ID, recipient name, failure time and a preview of
the first 100 Unicode characters. Existing failures from before this schema
upgrade are not backfilled.

The sender receives a status wake when connected and ready. Claude receives the
notification as channel data. Other harnesses receive an instruction to call
`list_notifications`, keeping the quoted preview out of their user prompt queue.
The agent reports the failure and calls `ack_notification`. Neither tool executes,
resends or acknowledges the original task.

Notification delivery is at least once across reconnects. AX sends one status
wake per connection until acknowledgment; if a wake fails, retries on that
connection wait 30 seconds. An unacknowledged notification may appear again after
reconnection. Its stable ID lets the recipient recognize it. Notifications never
request replies, consume message reply depth or trigger an automatic resend.

`list_notifications` returns the oldest 100 unacknowledged notifications for the
authenticated agent. Acknowledging them exposes the next batch. The sender can
also invoke it once when asked to recover missed status reports. It is not a
polling mechanism. Notification rows share the original message's seven-day
retention from creation, and cleanup removes them with that message.

Native wake handling runs separately from heartbeats so a slow harness does not
expire its own broker lease. Mail and status wakes remain serialized through the
bridge; there is one heartbeat worker per connection and no worker per message.

This introduces mailbox schema version 3. Older AX binaries reject that version
rather than silently ignoring notification state. Do not install an older broker
against an upgraded mailbox. The schema is upgraded when the new broker opens
the mailbox; this source change does not replace any running process.

## Relay sender notices

Relay senders also receive `acknowledged` after an explicit acknowledgment without
a reply, and `abandoned` after owner recovery. These are stored atomically with
the terminal state. A reply itself reaches the relay, so it does not produce an
extra acknowledgment notice. Ordinary session senders keep the existing expiry
and refusal notifications. MCP delivery carries the complete notice JSON with
`meta.kind: ax_delivery_status`; `ack_notification` and restart semantics are
unchanged. Relay support moves the mailbox to schema 4; older brokers refuse it.

## Stalled offers

Once a minute, the maintenance sweep finds messages at `wake_accepted` or `channel_written` whose latest event is older than the stall threshold while their local recipient is reported `ready`. The threshold is five minutes when the recipient's harness reports busy and idle turns, and fifteen minutes when it does not, such as Grok or a Codex session without native state observation. The sender gets one `stalled` notification per offer attempt, through the same status wake, `list_notifications` and `ack_notification` path. A busy or blocked recipient produces no notice.

An observed Codex peer must remain ready for the whole five-minute window. A turn can fail after consuming a wake without fetching its message, so a turn after the offer does not suppress the notice. Other harnesses with busy and idle observation must have been ready since before the offer. A broker restart begins a new observation window for Codex.

Messages with a live replacement from the same sender to the same recipient produce no new stalled notice. Expired, refused, abandoned or overdue queued replacements do not suppress it. Each message is checked separately, so an expired middle message can leave both earlier and later messages eligible.

The notice stores the message, recipient, delivery state, minutes since its last event and the offer attempt; guidance is rendered when read. AX does not offer the message again. Check the recipient, and if it is not working on the message, have it call `list_pending` once. A stalled notice stays as long as its message stays unresolved and is deleted with the message after retention.
