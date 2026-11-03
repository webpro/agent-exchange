package ax

import (
	"fmt"
	"time"
)

type ContextUsage struct {
	UsedTokens   *int64 `json:"context_used_tokens,omitempty"`
	WindowTokens *int64 `json:"context_window_tokens,omitempty"`
	ObservedAt   int64  `json:"context_observed_at_ms,omitempty"`
}

func (u *ContextUsage) valid() bool {
	return u != nil && u.ObservedAt > 0 && u.ObservedAt <= maxSequence &&
		(u.UsedTokens == nil || *u.UsedTokens >= 0 && *u.UsedTokens <= maxSequence) &&
		(u.WindowTokens == nil || *u.WindowTokens > 0 && *u.WindowTokens <= maxSequence)
}

func (u *ContextUsage) percent() *float64 {
	if !u.valid() || u.UsedTokens == nil || u.WindowTokens == nil {
		return nil
	}
	p := 100 * float64(*u.UsedTokens) / float64(*u.WindowTokens)
	return &p
}

func contextColumns(u *ContextUsage, now time.Time) string {
	if !u.valid() {
		return ""
	}
	out := ""
	if u.UsedTokens != nil {
		out = fmt.Sprintf(" context=%d", *u.UsedTokens)
		if u.WindowTokens != nil {
			out += fmt.Sprintf("/%d(%.2f%%)", *u.WindowTokens, *u.percent())
		}
	} else if u.WindowTokens != nil {
		out = fmt.Sprintf(" context_window=%d", *u.WindowTokens)
	}
	return out + " context_seen=" + compactAge(now.Sub(time.UnixMilli(u.ObservedAt)))
}
