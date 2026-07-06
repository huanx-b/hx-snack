package mother

import "time"

const (
	childReapInterval    = 15 * time.Second
	wsHeartbeatTimeout   = 75 * time.Second
	wsWriteWait          = 10 * time.Second
	httpHeartbeatTimeout = 90 * time.Second
)

func (h *Hub) reapStaleChildren() {
	ticker := time.NewTicker(childReapInterval)
	defer ticker.Stop()
	for range ticker.C {
		h.reapStaleChildrenOnce(time.Now())
	}
}

func (h *Hub) reapStaleChildrenOnce(now time.Time) {
	var stale []struct {
		id    string
		child *ChildState
	}

	h.mu.RLock()
	for id, child := range h.children {
		timeout, ok := childHeartbeatTimeout(child.Transport)
		if !ok {
			continue
		}
		child.mu.Lock()
		last := child.LastHeartbeat
		if last.IsZero() {
			last = child.ConnectedAt
		}
		child.mu.Unlock()
		if !last.IsZero() && now.Sub(last) > timeout {
			stale = append(stale, struct {
				id    string
				child *ChildState
			}{id: id, child: child})
		}
	}
	h.mu.RUnlock()

	for _, item := range stale {
		h.disconnectChild(item.id, item.child)
	}
}

func childHeartbeatTimeout(transport string) (time.Duration, bool) {
	switch transport {
	case "ws":
		return wsHeartbeatTimeout, true
	case "http":
		return httpHeartbeatTimeout, true
	default:
		return 0, false
	}
}
