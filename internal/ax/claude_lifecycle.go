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
	since int64
	until int64
	ended int64
}

func (b *broker) observeClaudeTurn(p *peer, turn *nativeTurn) (string, error) {
	if !validNativeTurn(turn) {
		return "", nil
	}
	window, observed := p.claudeTurns[turn.ID]
	if window.ended != 0 {
		return "", nil
	}
	if turn.Status == "inProgress" {
		if observed {
			return "", nil
		}
		window = claudeTurnWindow{start: turn.At, since: max(p.claudeEndedAt, p.enrolled.UnixMilli())}
		// Reports can arrive out of order. A start older than the newest one
		// seen belongs to an earlier turn: it leaves the current turn in place
		// and keeps a window for notice scope that ends where the next turn began.
		if turn.At < p.claudeStartedAt {
			for _, later := range p.claudeTurns {
				if later.start > turn.At && (window.until == 0 || later.start < window.until) {
					window.until = later.start
				}
			}
			p.rememberClaudeTurn(turn.ID, window)
			return "", nil
		}
		if p.claudeTurn != nil {
			if previous, ok := p.claudeTurns[p.claudeTurn.ID]; ok && previous.until == 0 {
				previous.until = turn.At
				p.claudeTurns[p.claudeTurn.ID] = previous
			}
		}
		p.rememberClaudeTurn(turn.ID, window)
		p.claudeStartedAt, p.claudeTurn = turn.At, turn
		return "busy", nil
	}
	if turn.Status == "interrupted" && observed {
		if err := b.noticeInterrupted(p, *turn, &window); err != nil {
			return "", err
		}
	} else if turn.Status != "interrupted" {
		p.claudeEndedAt = max(p.claudeEndedAt, turn.At)
		// A completion reported late still bounds the turns that started after it.
		for id, later := range p.claudeTurns {
			if later.start > turn.At && later.since < turn.At {
				later.since = turn.At
				p.claudeTurns[id] = later
			}
		}
	}
	window.ended = turn.At
	p.rememberClaudeTurn(turn.ID, window)
	if observed && p.claudeTurn != nil && p.claudeTurn.ID == turn.ID {
		p.claudeTurn = turn
		return "ready", nil
	}
	if !observed && (p.claudeTurn == nil || p.claudeTurn.Status != "inProgress") {
		return "ready", nil
	}
	return "", nil
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
