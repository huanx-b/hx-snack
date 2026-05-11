package mother

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"time"
)

//go:embed all:../../web
var webAssets embed.FS

// SetupRoutes registers all HTTP routes on the given mux.
func SetupRoutes(mux *http.ServeMux, hub *Hub, tm *TunnelManager) {
	// API routes
	mux.HandleFunc("/api/children", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, hub.ListChildren())
	})

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
			taskID := generateID()
			err := hub.SendTask(req.ChildID, taskID, req.Command, req.Args, req.Timeout)
			if err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, map[string]string{"task_id": taskID})

		case "GET":
			hub.taskMu.RLock()
			defer hub.taskMu.RUnlock()
			var results []map[string]interface{}
			for id, r := range hub.taskResults {
				results = append(results, map[string]interface{}{
					"task_id":   id,
					"exit_code": r.ExitCode,
					"stdout":    r.Stdout,
					"stderr":    r.Stderr,
					"duration":  r.Duration,
				})
			}
			writeJSON(w, results)

		default:
			http.Error(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/tunnels", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			var req TunnelRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			tunnel, err := tm.OpenTunnel(req.ID, req.ChildID, req.Target, req.ListenPort)
			if err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, tunnel)

		case "GET":
			writeJSON(w, tm.ListTunnels())

		case "DELETE":
			id := r.URL.Query().Get("id")
			if err := tm.CloseTunnel(id); err != nil {
				writeJSON(w, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, map[string]string{"status": "closed"})

		default:
			http.Error(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		count := hub.Count()
		tunnelCount := len(tm.ListTunnels())
		writeJSON(w, map[string]interface{}{
			"children": count,
			"tunnels":  tunnelCount,
			"uptime":   time.Now().Unix(),
		})
	})

	// SSE events
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		WriteSSE(w, hub.Events())
	})

	// WS endpoint for children
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

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

const indexFallback = `<!DOCTYPE html>
<html><head><title>hxの偷吃</title><meta charset="utf-8"></head>
<body style="background:#0b0b0f;color:#f5f5f5;font-family:Inter,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
<h1 style="font-weight:800;font-size:2rem">hxの偷吃 <span style="color:#d4143a">Mother</span> is online</h1>
</body></html>`