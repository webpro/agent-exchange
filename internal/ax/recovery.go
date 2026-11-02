package ax

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type requestFailure struct {
	cause     error
	submitted bool
	// An earlier attempt was submitted and lost its response, so the outcome stays
	// unknown whatever a later attempt reports.
	lost bool
}

func (e *requestFailure) Error() string { return e.cause.Error() }
func (e *requestFailure) Unwrap() error { return e.cause }

// Retry the whole setup on one replacement connection. The request arguments,
// including send/reply idempotency keys, stay unchanged across both attempts.
func (b *bridge) toolCall(ctx context.Context, meta json.RawMessage, method string, args object, out any) error {
	if err := b.startupCalled(); err != nil {
		return &requestFailure{cause: err}
	}
	submitted, lost := false, false
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		lost = submitted
		if err = resourcePause(b.dir, time.Now()); err != nil {
			break
		}
		var c *client
		c, err = b.connection(ctx)
		if err != nil {
			break
		}
		// Leave time for the existing reconnect backoff within the tool's total
		// deadline. A quiet broker cannot consume a fresh budget at each stage.
		attemptCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		err = c.callContext(attemptCtx, "ax.heartbeat", object{}, nil)
		if err == nil {
			err = b.bind(attemptCtx, c, meta)
		}
		if err == nil {
			err = b.activateClient(attemptCtx, c)
		}
		if err == nil {
			err = c.callContext(attemptCtx, method, args, out)
			var transport *transportError
			if err != nil {
				// Broker errors may include a commit failure; conservatively retain
				// uncertainty rather than assert that a sent write did not queue.
				submitted = submitted || !errors.As(err, &transport) || transport.submitted
			}
		}
		cancel()
		if err == nil {
			return nil
		}
		// A lost receive response may already have consumed the next queued item.
		// Recover it through list_pending, never silently receive another item.
		if method == "ax.check_inbox" && submitted {
			break
		}
		var transport *transportError
		if !errors.As(err, &transport) {
			break
		}
		b.invalidate(c)
	}
	return &requestFailure{cause: err, submitted: submitted, lost: lost}
}

func toolFailure(err error, args object) object {
	failure := object{"code": "request_failed", "message": err.Error(), "retryable": false, "submission": "not_applicable"}
	var transport *transportError
	if errors.As(err, &transport) {
		failure["code"] = "broker_unavailable"
		failure["retryable"] = true
		failure["connection_state"] = "reconnecting"
	}
	var paused *resourcePaused
	if errors.As(err, &paused) {
		failure["code"] = "resource_cooldown"
		failure["retryable"] = true
		failure["retry_after_ms"] = max(0, time.Until(paused.Until).Milliseconds())
	}
	if key, ok := args["client_message_id"]; ok {
		failure["client_message_id"] = key
		failure["submission"] = "not_submitted"
		var request *requestFailure
		if errors.As(err, &request) && request.submitted {
			failure["submission"] = "outcome_unknown"
		}
		failure["recovery"] = "If retrying this operation after recovery, reuse this client_message_id with unchanged arguments. Do not generate a new key."
	}
	var rpc *rpcError
	var request *requestFailure
	if _, keyed := args["client_message_id"]; keyed && errors.As(err, &rpc) && rpc.Code == requestRefusedCode && !(errors.As(err, &request) && request.lost) {
		// The broker refused this send before storing anything, and no earlier
		// attempt of the call lost its response, so nothing was sent.
		step, _ := rpc.Data["recovery"].(string)
		if step == "" {
			step = "Change the request as the error message says, then send again."
		}
		failure["code"] = "request_refused"
		failure["submission"] = "not_submitted"
		failure["recovery"] = "Nothing was sent. " + step
		if wait, ok := rpc.Data["retry_after_ms"]; ok {
			failure["retryable"] = true
			failure["retry_after_ms"] = wait
		}
	}
	if errors.As(err, &rpc) && rpc.Code == replyDepthLimitCode {
		// This rejection occurs before any write, unlike a generic broker error
		// which may report a failed commit with an uncertain outcome.
		failure["code"] = "reply_depth_limit"
		failure["submission"] = "not_submitted"
		for _, key := range []string{"target", "previous_message_id", "thread_id", "recovery"} {
			if value, ok := rpc.Data[key]; ok {
				failure[key] = value
			}
		}
	}
	if errors.As(err, &rpc) && rpc.Code == externalPolicyCode {
		failure["code"] = "external_policy"
		failure["rpc_code"] = rpc.Code
		failure["submission"] = "not_submitted"
		failure["retryable"] = false
		for key, value := range rpc.Data {
			failure[key] = value
		}
		failure["recovery"] = "The recipient owner must open its external policy before this message can be sent."
	}
	return failure
}
