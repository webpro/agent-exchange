package ax

type nativeTurn struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	At     int64  `json:"at_ms"`
}

func validNativeTurn(t *nativeTurn) bool {
	if t == nil || t.ID == "" || len(t.ID) > 128 || t.At <= 0 {
		return false
	}
	switch t.Status {
	case "inProgress", "completed", "failed", "interrupted":
		return true
	}
	return false
}

type claudeTurnWindow struct {
	start int64
	ended int64
}

func (p *peer) observeClaudeTurn(turn *nativeTurn) string {
	if !validNativeTurn(turn) {
		return ""
	}
	window, observed := p.claudeTurns[turn.ID]
	if window.ended != 0 {
		return ""
	}
	if turn.Status == "inProgress" {
		if observed {
			return ""
		}
		p.rememberClaudeTurn(turn.ID, claudeTurnWindow{start: turn.At})
		// Reports can arrive out of order. A start older than the newest one
		// seen belongs to an earlier turn and leaves the current turn in place.
		if turn.At < p.claudeStartedAt {
			return ""
		}
		p.claudeStartedAt, p.claudeTurn = turn.At, turn
		return "busy"
	}
	window.ended = turn.At
	p.rememberClaudeTurn(turn.ID, window)
	if observed && p.claudeTurn != nil && p.claudeTurn.ID == turn.ID {
		p.claudeTurn = turn
		return "ready"
	}
	if !observed && (p.claudeTurn == nil || p.claudeTurn.Status != "inProgress") {
		return "ready"
	}
	return ""
}

func (p *peer) rememberClaudeTurn(id string, window claudeTurnWindow) {
	if p.claudeTurns == nil {
		p.claudeTurns = make(map[string]claudeTurnWindow)
	}
	if _, exists := p.claudeTurns[id]; !exists && len(p.claudeTurns) >= 16 {
		var oldest string
		for candidate, previous := range p.claudeTurns {
			if p.claudeTurn != nil && p.claudeTurn.Status == "inProgress" && candidate == p.claudeTurn.ID {
				continue
			}
			first := p.claudeTurns[oldest]
			if oldest == "" || max(previous.start, previous.ended) < max(first.start, first.ended) {
				oldest = candidate
			}
		}
		delete(p.claudeTurns, oldest)
	}
	p.claudeTurns[id] = window
}
