package mother

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

// Tunnel represents an active port-forward tunnel.
type Tunnel struct {
	ID         string
	ChildID    string
	Target     string
	ListenPort int
	CreatedAt  time.Time
	BytesIn    uint64
	BytesOut   uint64
	mu         sync.Mutex
	cancel     chan struct{}
	pool       *TunnelPool // nil if standalone
}

// TunnelPool groups tunnels sharing the same port for load balancing.
type TunnelPool struct {
	Port      int
	Target    string
	listener  net.Listener
	backends  []*Tunnel
	nextIdx   uint64
	mu        sync.Mutex
	cancel    chan struct{}
}

// TunnelManager manages tunnels and pools.
type TunnelManager struct {
	mu      sync.RWMutex
	tunnels map[string]*Tunnel
	pools   map[int]*TunnelPool // port -> pool
	hub     *Hub
}

// NewTunnelManager creates a tunnel manager.
func NewTunnelManager(hub *Hub) *TunnelManager {
	return &TunnelManager{
		tunnels: make(map[string]*Tunnel),
		pools:   make(map[int]*TunnelPool),
		hub:     hub,
	}
}

// OpenTunnel starts a TCP listener and forwards traffic through the child's WS.
// If a pool already exists for this port, adds to the pool (load balanced).
func (tm *TunnelManager) OpenTunnel(id, childID, target string, listenPort int) (*Tunnel, error) {
	tm.hub.mu.RLock()
	_, ok := tm.hub.children[childID]
	tm.hub.mu.RUnlock()
	if !ok {
		return nil, ErrChildNotFound
	}

	t := &Tunnel{
		ID:         id,
		ChildID:    childID,
		Target:     target,
		ListenPort: listenPort,
		CreatedAt:  time.Now(),
		cancel:     make(chan struct{}),
	}

	tm.mu.Lock()

	// Check if a pool already exists for this port
	pool, exists := tm.pools[listenPort]
	if exists {
		// Add to existing pool
		pool.mu.Lock()
		pool.backends = append(pool.backends, t)
		pool.mu.Unlock()
		t.pool = pool
		tm.tunnels[id] = t
		tm.mu.Unlock()
		log.Printf("[tunnel] %s joined pool :%d (%d backends)", id, listenPort, len(pool.backends))
		return t, nil
	}

	// Create new listener and pool
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", listenPort))
	if err != nil {
		tm.mu.Unlock()
		return nil, fmt.Errorf("listen: %w", err)
	}

	pool = &TunnelPool{
		Port:     listenPort,
		Target:   target,
		listener: listener,
		backends: []*Tunnel{t},
		cancel:   make(chan struct{}),
	}
	t.pool = pool
	tm.pools[listenPort] = pool
	tm.tunnels[id] = t
	tm.mu.Unlock()

	// Start accept loop
	go tm.acceptLoop(pool)

	log.Printf("[tunnel] pool :%d created with %s", listenPort, id)
	return t, nil
}

func (tm *TunnelManager) acceptLoop(pool *TunnelPool) {
	defer pool.listener.Close()

	for {
		select {
		case <-pool.cancel:
			return
		default:
		}

		conn, err := pool.listener.Accept()
		if err != nil {
			select {
			case <-pool.cancel:
				return
			default:
				log.Printf("[tunnel] pool :%d accept error: %v", pool.Port, err)
				continue
			}
		}

		// Round-robin pick backend
		pool.mu.Lock()
		if len(pool.backends) == 0 {
			pool.mu.Unlock()
			conn.Close()
			continue
		}
		idx := atomic.AddUint64(&pool.nextIdx, 1) % uint64(len(pool.backends))
		backend := pool.backends[idx]
		pool.mu.Unlock()

		go tm.handleBidirConn(backend, conn)
	}
}

func (tm *TunnelManager) handleBidirConn(t *Tunnel, conn net.Conn) {
	defer conn.Close()

	tm.hub.mu.RLock()
	child, ok := tm.hub.children[t.ChildID]
	tm.hub.mu.RUnlock()
	if !ok {
		log.Printf("[tunnel] bidir: child %s not found", t.ChildID)
		return
	}

	// Generate a stream ID for this connection
	streamID := generateID()
	log.Printf("[tunnel] bidir: %s starting, child=%s target=%s", streamID, t.ChildID, t.Target)

	// Tell child to open tunnel to target
	openMsg := protocol.NewMessage(protocol.TypeTunnelOpen, protocol.TunnelOpenPayload{
		TunnelID: streamID,
		Target:   t.Target,
	})

	// Register handler for incoming data from child
	readyCh := tm.hub.RegisterTunnelStream(streamID, func(data []byte) {
		if conn != nil {
			n, _ := conn.Write(data)
			t.mu.Lock()
			t.BytesOut += uint64(n)
			t.mu.Unlock()
		}
	})
	log.Printf("[tunnel] bidir: %s registered stream", streamID)

	child.mu.Lock()
	b, _ := msgpack.Marshal(openMsg)
	err := child.Conn.WriteMessage(websocket.BinaryMessage, b)
	child.mu.Unlock()
	if err != nil {
		log.Printf("[tunnel] bidir: %s write tunnel_open error: %v", streamID, err)
		return
	}
	log.Printf("[tunnel] bidir: %s sent tunnel_open, waiting ready...", streamID)

	// Wait for ready signal or timeout
	select {
	case <-readyCh:
		log.Printf("[tunnel] bidir: %s ready!", streamID)
	case <-time.After(10 * time.Second):
		log.Printf("[tunnel] bidir: %s timeout waiting ready", streamID)
		return
	}

	// Read from TCP -> send to child
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-t.cancel:
			return
		default:
		}

		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		data := make([]byte, n)
		copy(data, buf[:n])

		msg := protocol.NewMessage(protocol.TypeTunnelData, protocol.TunnelDataPayload{
			TunnelID: streamID,
			Data:     data,
		})
		child.mu.Lock()
		mb, _ := msgpack.Marshal(msg)
		child.Conn.WriteMessage(websocket.BinaryMessage, mb)
		child.mu.Unlock()

		t.mu.Lock()
		t.BytesIn += uint64(n)
		t.mu.Unlock()
	}
}

// CloseTunnel closes a tunnel and removes from pool.
func (tm *TunnelManager) CloseTunnel(id string) error {
	tm.mu.Lock()
	t, ok := tm.tunnels[id]
	if !ok {
		tm.mu.Unlock()
		return fmt.Errorf("tunnel not found")
	}
	delete(tm.tunnels, id)

	if t.pool != nil {
		t.pool.mu.Lock()
		for i, b := range t.pool.backends {
			if b == t {
				t.pool.backends = append(t.pool.backends[:i], t.pool.backends[i+1:]...)
				break
			}
		}
		remaining := len(t.pool.backends)
		t.pool.mu.Unlock()

		// If pool is empty, clean it up
		if remaining == 0 {
			close(t.pool.cancel)
			t.pool.listener.Close()
			delete(tm.pools, t.ListenPort)
		}
	}
	tm.mu.Unlock()

	close(t.cancel)
	return nil
}

// ListTunnels returns all active tunnels.
func (tm *TunnelManager) ListTunnels() []TunnelInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var list []TunnelInfo
	for _, t := range tm.tunnels {
		t.mu.Lock()
		info := TunnelInfo{
			ID:         t.ID,
			ChildID:    t.ChildID,
			Target:     t.Target,
			ListenPort: t.ListenPort,
			BytesIn:    t.BytesIn,
			BytesOut:   t.BytesOut,
		}
		if t.pool != nil {
			t.pool.mu.Lock()
			info.PoolSize = len(t.pool.backends)
			t.pool.mu.Unlock()
		}
		t.mu.Unlock()
		list = append(list, info)
	}
	return list
}

type TunnelInfo struct {
	ID         string `json:"id"`
	ChildID    string `json:"child_id"`
	Target     string `json:"target"`
	ListenPort int    `json:"listen_port"`
	BytesIn    uint64 `json:"bytes_in"`
	BytesOut   uint64 `json:"bytes_out"`
	PoolSize   int    `json:"pool_size,omitempty"`
}

type TunnelRequest struct {
	ID         string `json:"id"`
	ChildID    string `json:"child_id"`
	Target     string `json:"target"`
	ListenPort int    `json:"listen_port"`
}

// startTunnel is called from hub when child sends tunnel_open.
func (h *Hub) startTunnel(child *ChildState, payload *protocol.TunnelOpenPayload) {
	log.Printf("[hub] tunnel %s acknowledged for child %s", payload.TunnelID, child.ID)
}

// RegisterTunnelStream lets tunnel manager register callbacks for child→mother tunnel data.
// Returns a channel that closes when tunnel is ready.
func (h *Hub) RegisterTunnelStream(streamID string, onData func([]byte)) <-chan struct{} {
	h.tunnelMu.Lock()
	if h.tunnelStreams == nil {
		h.tunnelStreams = make(map[string]chan []byte)
	}
	ch := make(chan []byte, 64)
	h.tunnelStreams[streamID] = ch
	h.tunnelMu.Unlock()

	ready := make(chan struct{})

	// Store ready channel
	h.tunnelMu.Lock()
	if h.tunnelReady == nil {
		h.tunnelReady = make(map[string]chan struct{})
	}
	h.tunnelReady[streamID] = ready
	h.tunnelMu.Unlock()

	go func() {
		defer func() {
			h.tunnelMu.Lock()
			delete(h.tunnelStreams, streamID)
			delete(h.tunnelReady, streamID)
			h.tunnelMu.Unlock()
		}()
		for data := range ch {
			onData(data)
		}
	}()

	return ready
}

// WriteSSE streams events as Server-Sent Events.
func WriteSSE(w http.ResponseWriter, events <-chan interface{}) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
		return
	}

	for evt := range events {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
}

// DiscardRead is a helper for tunnels.
var _ = io.Discard