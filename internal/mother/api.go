package mother

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"time"
)

//go:embed all:../../web
var webAssets embed.FS

// SetupRoutes registers all HTTP routes on the given mux.
func SetupRoutes(mux *http.ServeMux, hub *Hub, tm *TunnelManager) {
	// API — Children
	mux.HandleFunc("/api/children", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			children := hub.ListChildren()
			writeJSON(w, map[string]interface{}{
				"children": children,
				"count":    len(children),
			})
		case "DELETE":
			// Disconnect a child
			id := r.URL.Query().Get("id")
			hub.mu.Lock()
			if child, ok := hub.children[id]; ok {
				child.Conn.Close()
				delete(hub.children, id)
			}
			hub.mu.Unlock()
			writeJSON(w, map[string]string{"status": "disconnected"})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})

	// API — Tasks
	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			var req struct {
				ChildID string   `json:"child_id"`
				Command string   `json:"command"`
				Args    []string `json:"args"`
				Timeout int      `json:"timeout"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			record, err := hub.SubmitTask(req.ChildID, req.Command, req.Args, req.Timeout)
			if err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, record)

		case "GET":
			childID := r.URL.Query().Get("child_id")
			tasks := hub.ListTasks(childID)
			writeJSON(w, map[string]interface{}{
				"tasks": tasks,
				"count": len(tasks),
			})

		default:
			http.Error(w, "method not allowed", 405)
		}
	})

	// API — Single task
	mux.HandleFunc("/api/tasks/", func(w http.ResponseWriter, r *http.Request) {
		taskID := r.URL.Path[len("/api/tasks/"):]
		task := hub.GetTask(taskID)
		if task == nil {
			writeJSON(w, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, task)
	})

	// API — Tunnels
	mux.HandleFunc("/api/tunnels", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			var req TunnelRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			if req.ID == "" {
				req.ID = generateID()
			}
			tunnel, err := tm.OpenTunnel(req.ID, req.ChildID, req.Target, req.ListenPort)
			if err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, tunnel)

		case "GET":
			writeJSON(w, map[string]interface{}{
				"tunnels": tm.ListTunnels(),
				"count":   len(tm.ListTunnels()),
			})

		default:
			http.Error(w, "method not allowed", 405)
		}
	})

	// API — Single tunnel
	mux.HandleFunc("/api/tunnels/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "DELETE":
			tunnelID := r.URL.Path[len("/api/tunnels/"):]
			if err := tm.CloseTunnel(tunnelID); err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, map[string]string{"status": "closed"})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})

	// API — Stats
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		tasks := hub.ListTasks("")
		completed := 0
		for _, t := range tasks {
			if t.Status == TaskCompleted || t.Status == TaskFailed {
				completed++
			}
		}
		writeJSON(w, map[string]interface{}{
			"children":        hub.Count(),
			"tunnels":         len(tm.ListTunnels()),
			"tasks_total":     len(tasks),
			"tasks_completed": completed,
			"uptime":          time.Now().Unix(),
		})
	})

	// API — Events (SSE)
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		WriteSSE(w, hub.Events())
	})

	// WS for children
	mux.HandleFunc("/ws", hub.HandleWS)

	// WebUI
	webFS, err := fs.Sub(webAssets, "web")
	if err != nil {
		log.Printf("[mother] web assets not embedded: %v", err)
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" || r.URL.Path == "/index.html" {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.Write([]byte(indexFallback))
				return
			}
			http.NotFound(w, r)
		})
		return
	}
	mux.Handle("/", http.FileServer(http.FS(webFS)))
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

const indexFallback = `<!DOCTYPE html>
<html><head><title>hxの偷吃</title><meta charset="utf-8"></head>
<body style="background:#0b0b0f;color:#f5f5f5;font-family:Inter,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
<h1 style="font-weight:800;font-size:2rem">hxの偷吃 <span style="color:#d4143a">Mother</span> is online</h1>
</body></html>`