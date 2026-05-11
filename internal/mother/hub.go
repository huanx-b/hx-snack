package mother

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
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
	ID           string
	Hostname     string
	OS           string
	Arch         string
	Version      string
	Conn         *websocket.Conn
	ConnectedAt  time.Time
	LastHeartbeat time.Time
	LastReport   *protocol.ReportPayload
	mu           sync.Mutex
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
}

// NewHub creates a new Hub.
func NewHub(psk string) *Hub {
	h := &Hub{
		children: make(map[string]*ChildState),
		psk:      psk,
		events:   make(chan interface{}, 256),
	}
	h.taskQueue = NewTaskQueue(h)
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

	childID := generateID()
	child := &ChildState{
		ID:          childID,
		Conn:        conn,
		ConnectedAt: time.Now(),
	}
	h.mu.Lock()
	h.children[childID] = child
	h.mu.Unlock()
	log.Printf("[hub] child %s connected", childID)

	defer func() {
		conn.Close()
		h.mu.Lock()
		delete(h.children, childID)
		h.mu.Unlock()
		log.Printf("[hub] child %s disconnected", childID)
		h.broadcastEvent("child_disconnected", childID)
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

	case protocol.TypeTunnelOpen:
		var payload protocol.TunnelOpenPayload
		h.decodePayload(msg.Payload, &payload)
		log.Printf("[hub] child %s tunnel %s -> %s", child.ID, payload.TunnelID, payload.Target)
		// Tunnel logic: open a local listener -> forward through WS
		go h.startTunnel(child, &payload)

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
		c.mu.Lock()
		info := ChildInfo{
			ID:        c.ID,
			Hostname:  c.Hostname,
			OS:        c.OS,
			Arch:      c.Arch,
			Version:   c.Version,
			Connected: c.ConnectedAt,
			LastHB:    c.LastHeartbeat,
		}
		if c.LastReport != nil {
			info.CPU = c.LastReport.CPUPercent
			info.MemUsed = c.LastReport.MemUsedBytes
			info.MemTotal = c.LastReport.MemTotalBytes
			info.Uptime = c.LastReport.UptimeSeconds
		}
		c.mu.Unlock()
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
	data, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.Conn.WriteMessage(websocket.BinaryMessage, data)
}

func (h *Hub) broadcastEvent(eventType string, data interface{}) {
	select {
	case h.events <- map[string]interface{}{"type": eventType, "data": data}:
	default:
	}
}

func (h *Hub) decodePayload(src, dst interface{}) {
	// msgpack packs payload as map; re-encode and decode to target struct
	b, _ := msgpack.Marshal(src)
	msgpack.Unmarshal(b, dst)
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ChildInfo for API responses.
type ChildInfo struct {
	ID        string    `json:"id"`
	Hostname  string    `json:"hostname"`
	OS        string    `json:"os"`
	Arch      string    `json:"arch"`
	Version   string    `json:"version"`
	Connected time.Time `json:"connected_at"`
	LastHB    time.Time `json:"last_heartbeat"`
	CPU       float64   `json:"cpu"`
	MemUsed   uint64    `json:"mem_used"`
	MemTotal  uint64    `json:"mem_total"`
	Uptime    int64     `json:"uptime"`
}

var ErrChildNotFound = &childNotFoundError{}

type childNotFoundError struct{}

func (e *childNotFoundError) Error() string { return "child not found" }