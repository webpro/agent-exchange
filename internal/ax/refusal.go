package ax

import "errors"

// A refusal is returned before the broker stores or changes any message, and
// only for send, reply, resend and follow_up, whose bridges report it as not
// submitted. The reason stays in the broker data for tests and logs; bridges
// pass on only the recovery step and a rate limit's wait. The message text is
// unchanged, so owner commands and older bridges see the same errors.
var refusalRecovery = map[string]string{
	"invalid_parameters":        "Check the argument types against the tool schema, then call again.",
	"invalid_ttl":               "Send again with an integer ttl_seconds from 1 to 604800, or omit it.",
	"invalid_client_message_id": "Use a client_message_id of 1 to 80 letters, digits, underscores or hyphens, or omit it.",
	"invalid_text":              "Send again with 1 to 65536 bytes of UTF-8 text.",
	"invalid_content":           "Fix the provenance or data field named in the message, then send again.",
	"invalid_replaces":          "Send again without replaces, or name your own earlier message to the same local recipient that has not ended.",
	"key_conflict":              "This client_message_id belongs to an earlier message with other arguments: repeat that message with its original arguments, or send the new content with a new client_message_id.",
	"sender_unapproved":         "The owner must relaunch this attached endpoint with an approved permission mode, or attach it with --relay.",
	"unknown_message":           "Check the message ID; it must name a message you sent or received.",
	"not_sender":                "Only the sender of a message can resend it or follow up on it.",
	"not_recipient":             "Only the recipient of a message can reply to it; add to your own request with follow_up.",
	"not_expired":               "Only an expired message can be resent; add to one still in progress with follow_up.",
	"not_offered":               "The message has not been offered to you yet; reply after its wake.",
	"already_terminal":          "The message is already in a final state; send a new message if anything remains to be done.",
	"unknown_target":            "Check list_agents for the recipient's name or agent_id, then send again.",
	"ambiguous_target":          "Several agents share this name; call list_agents and send to the agent_id.",
	"invalid_target":            "Send to another agent; a message cannot be addressed to this session.",
	"remote_route":              "This route to the paired machine is not authorized; send to a local agent, or have the owner check the messaging grant.",
	"recipient_refuses":         "The recipient refuses messages; have its owner set its policy to accept before sending again.",
	"mailbox_full":              "The mailbox is at its unresolved capacity; have the owner resolve or expire old mail before sending again.",
	"rate_limit":                "Wait until retry_after_ms has passed, then send again with the same arguments.",
	"repeated_text":             "The same text went to this recipient within the last ten seconds; change the text or wait, or reuse the original client_message_id if this is a retry of that message.",
}

func refuse(reason, message string, data object) *rpcError {
	if data == nil {
		data = object{}
	}
	data["reason"], data["recovery"] = reason, refusalRecovery[reason]
	return &rpcError{Code: requestRefusedCode, Message: message, Data: data}
}

// A target that names no agent, or several. A send turns it into a refusal;
// owner commands report the same text.
type targetError struct{ reason, text string }

func (e *targetError) Error() string { return e.text }

var errMessageNotFound = errors.New("message not found")

// A send naming a message the broker cannot find is refused; any other lookup
// error keeps the generic code.
func unknownMessage(err error) error {
	if errors.Is(err, errMessageNotFound) {
		return refuse("unknown_message", err.Error(), nil)
	}
	return err
}

func keyedWrite(method string) bool {
	return method == "ax.send" || method == "ax.reply" || method == "ax.resend" || method == "ax.follow_up"
}
