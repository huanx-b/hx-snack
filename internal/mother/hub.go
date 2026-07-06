package mother

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ChildState stores live state for a connected child.
type ChildState struct {
	ID            string
	Hostname      string
	OS            string
	Arch          string
	Version       string
	RemoteAddr    string
	Transport     string
	Conn          *websocket.Conn
	ConnectedAt   time.Time
	LastHeartbeat time.Time
	LastReport    *protocol.ReportPayload
	mu            sync.Mutex
}

// Hub manages all connected children.
type Hub struct {
	mu       sync.RWMutex
	children map[string]*ChildState

	// Tasks
	taskQueue *TaskQueue

	// PSK for simple auth
	psk string

	// Events for WebUI push
	events chan interface{}

	// Tunnel streams
	tunnelMu      sync.RWMutex
	tunnelStreams map[string]chan []byte
	tunnelReady   map[string]chan struct{}

	// HTTP long-poll child transport
	httpMu     sync.RWMutex
	httpQueues map[string]chan protocol.Message
}

// NewHub creates a new Hub.
func NewHub(psk string) *Hub {
	h := &Hub{
		children: make(map[string]*ChildState),
		psk:      psk,
		events:   make(chan interface{}, 256),
	}
	h.taskQueue = NewTaskQueue(h)
	go h.reapStaleChildren()
	return h
}

// HandleWS handles a WebSocket upgrade and manages the child lifecycle.
func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	// Simple PSK auth via query param
	if h.psk != "" && r.URL.Query().Get("key") != h.psk {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[hub] upgrade error: %v", err)
		return
	}

	clientIP := clientIPFromRequest(r)
	childID := childIDFromIP(clientIP)
	child := &ChildState{
		ID:          childID,
		RemoteAddr:  clientIP,
		Transport:   "ws",
		Conn:        conn,
		ConnectedAt: time.Now(),
	}
	replaced := h.replaceChild(child)
	if replaced {
		log.Printf("[hub] child %s reconnected from %s", childID, clientIP)
	} else {
		log.Printf("[hub] child %s connected from %s", childID, clientIP)
	}

	defer func() {
		conn.Close()
		removed := false
		h.mu.Lock()
		if cur, ok := h.children[childID]; ok && cur == child && cur.Conn == conn {
			delete(h.children, childID)
			removed = true
		}
		h.mu.Unlock()
		if removed {
			log.Printf("[hub] child %s disconnected", childID)
			h.broadcastEvent("child_disconnected", childID)
		} else {
			log.Printf("[hub] stale child connection %s closed", childID)
		}
	}()

	// Read loop
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[hub] child %s read error: %v", childID, err)
			return
		}

		var msg protocol.Message
		if err := msgpack.Unmarshal(raw, &msg); err != nil {
			log.Printf("[hub] unmarshal error: %v", err)
			continue
		}

		msg.ChildID = childID
		h.handleMessage(child, &msg)
	}
}

func (h *Hub) handleMessage(child *ChildState, msg *protocol.Message) {
	switch msg.Type {
	case protocol.TypeHeartbeat:
		child.mu.Lock()
		child.LastHeartbeat = time.Now()
		child.mu.Unlock()
		// Echo heartbeat back
		h.send(child, protocol.NewMessage(protocol.TypeHeartbeat, nil))

	case protocol.TypeRegister:
		var payload protocol.RegisterPayload
		if data, _ := json.Marshal(msg.Payload); true {
			json.Unmarshal(data, &payload)
		}
		// actual decoding via msgpack
		h.decodePayload(msg.Payload, &payload)

		child.mu.Lock()
		child.Hostname = payload.Hostname
		child.OS = payload.OS
		child.Arch = payload.Arch
		child.Version = payload.Version
		child.LastHeartbeat = time.Now()
		child.mu.Unlock()

		resp := protocol.NewMessage(protocol.TypeRegistered, protocol.RegisteredPayload{
			ChildID:    child.ID,
			HeartbeatS: 5,
		})
		h.send(child, resp)
		log.Printf("[hub] child %s registered: %s (%s/%s)", child.ID, payload.Hostname, payload.OS, payload.Arch)
		h.broadcastEvent("child_registered", map[string]interface{}{
			"id": child.ID, "hostname": payload.Hostname, "os": payload.OS, "arch": payload.Arch,
		})

	case protocol.TypeReport:
		var payload protocol.ReportPayload
		h.decodePayload(msg.Payload, &payload)
		child.mu.Lock()
		child.LastReport = &payload
		child.LastHeartbeat = time.Now()
		child.mu.Unlock()
		h.broadcastEvent("child_report", map[string]interface{}{
			"id": child.ID, "report": &payload,
		})

	case protocol.TypeTaskResult:
		var payload protocol.TaskResultPayload
		h.decodePayload(msg.Payload, &payload)
		h.taskQueue.CompleteTask(payload.TaskID, &payload)
		h.broadcastEvent("task_completed", map[string]interface{}{
			"task_id": payload.TaskID, "exit_code": payload.ExitCode, "child_id": child.ID,
		})

	case protocol.TypeTunnelData:
		var payload protocol.TunnelDataPayload
		h.decodePayload(msg.Payload, &payload)
		h.tunnelMu.RLock()
		ch, ok := h.tunnelStreams[payload.TunnelID]
		h.tunnelMu.RUnlock()
		if ok {
			select {
			case ch <- payload.Data:
			default:
			}
		}

	case protocol.TypeTunnelOpen:
		var payload protocol.TunnelOpenPayload
		h.decodePayload(msg.Payload, &payload)
		log.Printf("[hub] child %s tunnel %s -> %s", child.ID, payload.TunnelID, payload.Target)
		go h.startTunnel(child, &payload)

	case protocol.TypeTunnelReady:
		var payload protocol.TunnelOpenPayload
		h.decodePayload(msg.Payload, &payload)
		h.tunnelMu.Lock()
		if ch, ok := h.tunnelReady[payload.TunnelID]; ok {
			close(ch)
			delete(h.tunnelReady, payload.TunnelID)
		}
		h.tunnelMu.Unlock()

	default:
		log.Printf("[hub] unknown message type: %s from %s", msg.Type, child.ID)
	}
}

// SubmitTask submits a task to a child via the queue.
func (h *Hub) SubmitTask(childID, command string, args []string, timeout int) (*TaskRecord, error) {
	return h.taskQueue.Submit(childID, command, args, timeout)
}

// ListTasks returns all tasks.
func (h *Hub) ListTasks(childID string) []*TaskRecord {
	return h.taskQueue.ListTasks(childID)
}

// GetTask returns a task by ID.
func (h *Hub) GetTask(taskID string) *TaskRecord {
	return h.taskQueue.GetTask(taskID)
}

// sendTask sends a task payload directly to a child (used internally by TaskQueue).
func (h *Hub) sendTask(childID, taskID, command string, args []string, timeout int) error {
	h.mu.RLock()
	child, ok := h.children[childID]
	h.mu.RUnlock()
	if !ok {
		return ErrChildNotFound
	}

	msg := protocol.NewMessage(protocol.TypeTask, protocol.TaskPayload{
		TaskID:  taskID,
		Command: command,
		Args:    args,
		Timeout: timeout,
	})
	msg.ID = taskID
	return h.send(child, msg)
}

// ListChildren returns current child states.
func (h *Hub) ListChildren() []ChildInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var list []ChildInfo
	for _, c := range h.children {
		locked := c.mu.TryLock()
		info := ChildInfo{
			ID:         c.ID,
			Hostname:   c.Hostname,
			OS:         c.OS,
			Arch:       c.Arch,
			Version:    c.Version,
			RemoteAddr: c.RemoteAddr,
			Transport:  c.Transport,
			Connected:  c.ConnectedAt,
		}
		if locked {
			info.LastHB = c.LastHeartbeat
			if c.LastReport != nil {
				info.CPU = c.LastReport.CPUPercent
				info.MemUsed = c.LastReport.MemUsedBytes
				info.MemTotal = c.LastReport.MemTotalBytes
				info.NetRx = c.LastReport.NetRxBytes
				info.NetTx = c.LastReport.NetTxBytes
				info.Uptime = c.LastReport.UptimeSeconds
			}
			c.mu.Unlock()
		}
		list = append(list, info)
	}
	return list
}

// Count returns online child count.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.children)
}

// Events channel for SSE push.
func (h *Hub) Events() <-chan interface{} {
	return h.events
}

func (h *Hub) send(child *ChildState, msg protocol.Message) error {
	if child.Transport == "http" {
		return h.sendHTTP(child, msg)
	}
	data, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}
	child.mu.Lock()
	if err := child.Conn.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
		child.mu.Unlock()
		h.disconnectChild(child.ID, child)
		return err
	}
	err = child.Conn.WriteMessage(websocket.BinaryMessage, data)
	child.mu.Unlock()
	if err != nil {
		h.disconnectChild(child.ID, child)
	}
	return err
}

func (h *Hub) broadcastEvent(eventType string, data interface{}) {
	select {
	case h.events <- map[string]interface{}{"type": eventType, "data": data}:
	default:
	}
}

func (h *Hub) replaceChild(child *ChildState) bool {
	h.mu.Lock()
	old := h.children[child.ID]
	h.children[child.ID] = child
	h.mu.Unlock()

	if old == nil || old == child {
		return false
	}
	h.closeChildTransport(old.ID, old)
	return true
}

func (h *Hub) closeChildTransport(childID string, child *ChildState) {
	if child.Transport == "http" {
		h.closeHTTPQueue(childID)
		return
	}
	if child.Conn != nil {
		child.Conn.Close()
	}
}

func (h *Hub) disconnectChild(childID string, expected ...*ChildState) bool {
	h.mu.RLock()
	child := h.children[childID]
	h.mu.RUnlock()
	if child == nil {
		return false
	}
	if len(expected) > 0 && expected[0] != nil && child != expected[0] {
		return false
	}

	if child.Transport == "http" {
		h.removeHTTPChild(childID, child)
		return true
	}

	removed := false
	h.mu.Lock()
	if cur, ok := h.children[childID]; ok && cur == child {
		delete(h.children, childID)
		removed = true
	}
	h.mu.Unlock()
	if !removed {
		return false
	}
	if child.Conn != nil {
		child.Conn.Close()
	}
	h.broadcastEvent("child_disconnected", childID)
	return true
}

func (h *Hub) decodePayload(src, dst interface{}) {
	jb, _ := json.Marshal(src)
	if err := json.Unmarshal(jb, dst); err == nil {
		return
	}
	b, _ := msgpack.Marshal(src)
	msgpack.Unmarshal(b, dst)
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func clientIPFromRequest(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for _, part := range strings.Split(xff, ",") {
			if ip := cleanClientIP(part); ip != "" {
				return ip
			}
		}
	}
	if xrip := cleanClientIP(r.Header.Get("X-Real-IP")); xrip != "" {
		return xrip
	}
	if ip := cleanClientIP(r.RemoteAddr); ip != "" {
		return ip
	}
	return "unknown"
}

func cleanClientIP(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	addr = strings.Trim(addr, "[]")
	if addr == "" || strings.EqualFold(addr, "unknown") {
		return ""
	}
	return addr
}

func childIDFromIP(ip string) string {
	ip = cleanClientIP(ip)
	if ip == "" {
		ip = "unknown"
	}
	replacer := strings.NewReplacer(".", "_", ":", "_", "%", "_")
	return "ip_" + replacer.Replace(ip)
}

// ChildInfo for API responses.
type ChildInfo struct {
	ID         string    `json:"id"`
	Hostname   string    `json:"hostname"`
	OS         string    `json:"os"`
	Arch       string    `json:"arch"`
	Version    string    `json:"version"`
	RemoteAddr string    `json:"remote_addr"`
	Transport  string    `json:"transport"`
	Connected  time.Time `json:"connected_at"`
	LastHB     time.Time `json:"last_heartbeat"`
	CPU        float64   `json:"cpu"`
	MemUsed    uint64    `json:"mem_used"`
	MemTotal   uint64    `json:"mem_total"`
	NetRx      uint64    `json:"net_rx"`
	NetTx      uint64    `json:"net_tx"`
	Uptime     int64     `json:"uptime"`
}

var ErrChildNotFound = &childNotFoundError{}

type childNotFoundError struct{}

func (e *childNotFoundError) Error() string { return "child not found" }
