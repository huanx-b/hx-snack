package mother

import "time"

func (h *Hub) reapHTTPChildren() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		var stale []string
		h.mu.RLock()
		for id, child := range h.children {
			if child.Transport != "http" {
				continue
			}
			child.mu.Lock()
			last := child.LastHeartbeat
			child.mu.Unlock()
			if !last.IsZero() && time.Since(last) > 90*time.Second {
				stale = append(stale, id)
			}
		}
		h.mu.RUnlock()

		for _, id := range stale {
			h.removeHTTPChild(id)
		}
	}
}
