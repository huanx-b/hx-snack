package child

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

// Agent is the child-side client.
type Agent struct {
	MotherURL string
	PSK       string
	Version   string

	conn      *websocket.Conn
	mu        sync.Mutex
	childID   string
	stop      chan struct{}
	reconnect time.Duration
	monitor   *Monitor
}

// NewAgent creates a new child agent.
func NewAgent(motherURL, psk, version string) *Agent {
	return &Agent{
		MotherURL: motherURL,
		PSK:       psk,
		Version:   version,
		stop:      make(chan struct{}),
		reconnect: 1 * time.Second,
		monitor:   NewMonitor(),
	}
}

// Run starts the agent loop.
func (a *Agent) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.stop:
			return nil
		default:
		}

		err := a.connect(ctx)
		if err != nil {
			log.Printf("[child] connection failed: %v, retrying in %v", err, a.reconnect)
			select {
			case <-time.After(a.reconnect):
				a.reconnect = min(a.reconnect*2, 60*time.Second)
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		// If connection ended cleanly, reset backoff
		a.reconnect = 1 * time.Second
	}
}

func (a *Agent) connect(ctx context.Context) error {
	url := a.MotherURL
	if a.PSK != "" {
		if url[len(url)-1] != '?' {
			url += "?"
		}
		url += "key=" + a.PSK
	}

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	a.mu.Lock()
	a.conn = conn
	a.mu.Unlock()

	log.Printf("[child] connected to mother: %s", a.MotherURL)

	// Register
	hostname, _ := os.Hostname()
	reg := protocol.NewMessage(protocol.TypeRegister, protocol.RegisterPayload{
		Hostname: hostname,
		OS:       runtime.GOOS,
		Arch:     runtime.GOARCH,
		Version:  a.Version,
	})
	if err := a.send(reg); err != nil {
		return err
	}

	// Start heartbeat goroutine
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go a.heartbeatLoop(ctx)

	// Start monitor report goroutine
	go a.monitorLoop(ctx)

	// Read loop
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var msg protocol.Message
		if err := msgpack.Unmarshal(raw, &msg); err != nil {
			log.Printf("[child] unmarshal error: %v", err)
			continue
		}

		a.handleMessage(&msg)
	}
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := protocol.NewMessage(protocol.TypeHeartbeat, protocol.HeartbeatPayload{Seq: seq})
			if err := a.send(msg); err != nil {
				log.Printf("[child] heartbeat error: %v", err)
				return
			}
			seq++
		}
	}
}

func (a *Agent) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report := a.monitor.Collect()
			msg := protocol.NewMessage(protocol.TypeReport, report)
			if err := a.send(msg); err != nil {
				return
			}
		}
	}
}

func (a *Agent) handleMessage(msg *protocol.Message) {
	switch msg.Type {
	case protocol.TypeRegistered:
		var payload protocol.RegisteredPayload
		decode(msg.Payload, &payload)
		a.childID = payload.ChildID
		log.Printf("[child] registered as %s", payload.ChildID)

	case protocol.TypeHeartbeat:
		// Echo back from mother, ignore

	case protocol.TypeTask:
		var payload protocol.TaskPayload
		decode(msg.Payload, &payload)
		go a.executeTask(&payload)

	case protocol.TypeTunnelOpen:
		var payload protocol.TunnelOpenPayload
		decode(msg.Payload, &payload)
		log.Printf("[child] tunnel request: %s -> %s", payload.TunnelID, payload.Target)
		// Tunnel from child side would connect to target and forward
		// For v1, not implemented on child side

	default:
		log.Printf("[child] unknown message: %s", msg.Type)
	}
}

func (a *Agent) executeTask(task *protocol.TaskPayload) {
	log.Printf("[child] executing task %s: %s %v", task.TaskID, task.Command, task.Args)

	start := time.Now()
	var cancel context.CancelFunc
	ctx := context.Background()
	if task.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(task.Timeout)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, task.Command, task.Args...)
	if task.Env != nil {
		for k, v := range task.Env {
			cmd.Env = append(os.Environ(), k+"="+v)
		}
	}

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	var outBuf, errBuf []byte
	// Simple run — read all output
	cmd.Start()
	outBuf, _ = readAll(stdout)
	errBuf, _ = readAll(stderr)
	err := cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	result := protocol.NewMessage(protocol.TypeTaskResult, protocol.TaskResultPayload{
		TaskID:   task.TaskID,
		ExitCode: exitCode,
		Stdout:   string(outBuf),
		Stderr:   string(errBuf),
		Duration: time.Since(start).Milliseconds(),
	})

	if err := a.send(result); err != nil {
		log.Printf("[child] failed to send task result: %v", err)
	}
}

func (a *Agent) send(msg protocol.Message) error {
	data, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.conn == nil {
		return nil
	}
	return a.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (a *Agent) Close() {
	close(a.stop)
	a.mu.Lock()
	if a.conn != nil {
		a.conn.Close()
	}
	a.mu.Unlock()
}

func decode(src, dst interface{}) {
	b, _ := msgpack.Marshal(src)
	msgpack.Unmarshal(b, dst)
}

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, 4096)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			return buf, err
		}
	}
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}