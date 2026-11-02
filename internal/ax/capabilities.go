package ax

import (
	"errors"
	"fmt"
	"slices"
)

// Negotiated by the native bridge, never through model-facing tool arguments.
// A capability describes transport behavior; it grants no permissions.
type deliveryCapabilities struct {
	Version        int      `json:"version"`
	AdapterVersion string   `json:"adapter_version"`
	Content        string   `json:"content"`
	Boundary       string   `json:"boundary"`
	Boundaries     []string `json:"supported_boundaries"`
	Lifecycle      []string `json:"observable_lifecycle"`
}

func sessionCapabilities(s Session) (*deliveryCapabilities, error) {
	c := &deliveryCapabilities{Version: 1, AdapterVersion: Version, Content: "wake_fetch",
		Boundary: "native_queue", Boundaries: []string{"native_queue"}, Lifecycle: []string{"session_bound"}}
	switch s.Host {
	case "claude":
		c.Content = "peer_channel"
		c.Boundaries = append(c.Boundaries, "idle")
		c.Lifecycle = append(c.Lifecycle, "busy", "idle", "blocked")
	case "pi":
		c.Content, c.Boundary = "peer_channel", "turn_end"
		c.Boundaries = []string{"turn_end", "idle"}
		c.Lifecycle = append(c.Lifecycle, "busy", "idle")
	case "opencode":
		// promptAsync is not a peer channel or a reliable busy-session queue.
		c.Boundary, c.Boundaries = "idle", []string{"idle"}
		c.Lifecycle = append(c.Lifecycle, "busy", "idle", "blocked")
	case "codex", "grok":
		// Codex's launcher augments this contract after observing native state.
		// A tool call or binding alone cannot prove an idle transition.
	default:
		return nil, fmt.Errorf("no delivery contract for harness %q", s.Host)
	}
	if s.DeliveryBoundary != "" {
		if !slices.Contains(c.Boundaries, s.DeliveryBoundary) {
			return nil, fmt.Errorf("%s does not support AX delivery boundary %q; supported: %v", s.Host, s.DeliveryBoundary, c.Boundaries)
		}
		c.Boundary = s.DeliveryBoundary
	}
	return c, nil
}

func validateCapabilities(host string, c *deliveryCapabilities) error {
	if c == nil { // Older bridges retain their original delivery behavior.
		return nil
	}
	want, err := sessionCapabilities(Session{Host: host, DeliveryBoundary: c.Boundary})
	if err != nil {
		return err
	}
	if c.Version != 1 || c.Content != want.Content || c.AdapterVersion == "" || len(c.AdapterVersion) > 80 ||
		!slices.Equal(c.Boundaries, want.Boundaries) || !slices.Equal(c.Lifecycle, want.Lifecycle) {
		return errors.New("unsupported AX delivery capability contract")
	}
	return nil
}

func boundaryWait(p *peer) bool {
	return p.capabilities != nil && p.capabilities.Boundary == "idle" && p.State != "ready"
}

func observedCapabilities(p *peer) *deliveryCapabilities {
	if p.capabilities == nil || !p.codexObserved {
		return p.capabilities
	}
	c := *p.capabilities
	c.Lifecycle = []string{"session_bound", "busy", "idle"}
	return &c
}
