package child

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
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

	// SSH tunnel
	SSHTunnel  bool
	SSHHost    string
	SSHPort    string
	SSHUser    string
	SSHKey     string
	SSHPass    string
	TunnelPort string

	conn       *websocket.Conn
	httpBase   string
	httpMode   bool
	httpClient *http.Client
	mu         sync.Mutex
	childID    string
	stop       chan struct{}
	reconnect  time.Duration
	monitor    *Monitor

	tunnelMu      sync.RWMutex
	tunnelStreams map[string]chan []byte

	tunnelCmd *exec.Cmd // SSH tunnel process
}

// NewAgent creates a new child agent.
func NewAgent(motherURL, psk, version string) *Agent {
	return &Agent{
		MotherURL:  motherURL,
		PSK:        psk,
		Version:    version,
		stop:       make(chan struct{}),
		reconnect:  1 * time.Second,
		monitor:    NewMonitor(),
		httpClient: &http.Client{Timeout: 70 * time.Second},
	}
}

// Run starts the agent loop.
func (a *Agent) Run(ctx context.Context) error {
	// Start SSH tunnel if enabled
	if a.SSHTunnel {
		if err := a.startSSHTunnel(); err != nil {
			log.Printf("SSH tunnel failed: %v (will retry)", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.stop:
			return nil
		default:
		}

		// Check tunnel health if enabled
		if a.SSHTunnel && a.tunnelCmd != nil {
			if a.tunnelCmd.ProcessState != nil && a.tunnelCmd.ProcessState.Exited() {
				a.stopSSHTunnel()
				time.Sleep(1 * time.Second)
				if err := a.startSSHTunnel(); err != nil {
					log.Printf("SSH tunnel reconnect failed: %v", err)
				}
			}
		}

		err := a.connect(ctx)
		if err != nil {
			log.Printf("connection failed: %v, retrying in %v", err, a.reconnect)
			select {
			case <-time.After(a.reconnect):
				if a.reconnect*2 < 60*time.Second {
					a.reconnect = a.reconnect * 2
				} else {
					a.reconnect = 60 * time.Second
				}
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
	if strings.HasPrefix(a.MotherURL, "http://") || strings.HasPrefix(a.MotherURL, "https://") {
		return a.connectHTTP(ctx)
	}
	url := a.MotherURL
	// Route through SSH tunnel
	if a.SSHTunnel && a.TunnelPort != "" {
		url = replaceHostPort(url, "localhost:"+a.TunnelPort)
	}
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
	defer a.closeConn(conn)

	if err := conn.SetReadDeadline(time.Now().Add(wsReadWait)); err != nil {
		return err
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsReadWait))
	})

	a.mu.Lock()
	a.conn = conn
	a.mu.Unlock()

	log.Printf("connected")

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
		if err := conn.SetReadDeadline(time.Now().Add(wsReadWait)); err != nil {
			return err
		}

		var msg protocol.Message
		if err := msgpack.Unmarshal(raw, &msg); err != nil {
			// silent
			continue
		}

		a.handleMessage(&msg)
	}
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	var seq int64
	for {
		// Random interval 8-25s to avoid predictable patterns
		interval := 8*time.Second + time.Duration(randInt(17000))*time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			msg := protocol.NewMessage(protocol.TypeHeartbeat, protocol.HeartbeatPayload{Seq: seq})
			if err := a.send(msg); err != nil {
				a.closeCurrentConn()
				return // silent, don't log heartbeat errors
			}
			seq++
		}
	}
}

func randInt(n int) int {
	b := make([]byte, 4)
	rand.Read(b)
	return int(uint32(b[0])|uint32(b[1])<<8|uint32(b[2])<<16|uint32(b[3])<<24) % n
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
				a.closeCurrentConn()
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
		a.reconnect = 1 * time.Second
		// silent

	case protocol.TypeHeartbeat:
		// echo, ignore

	case protocol.TypeTask:
		var payload protocol.TaskPayload
		decode(msg.Payload, &payload)
		go a.executeTask(&payload)

	case protocol.TypeTunnelOpen:
		var payload protocol.TunnelOpenPayload
		decode(msg.Payload, &payload)
		// silent
		go a.handleTunnel(&payload)

	case protocol.TypeTunnelData:
		var payload protocol.TunnelDataPayload
		decode(msg.Payload, &payload)
		a.tunnelMu.RLock()
		ch, ok := a.tunnelStreams[payload.TunnelID]
		a.tunnelMu.RUnlock()
		if ok {
			select {
			case ch <- payload.Data:
			default:
			}
		}

	default:
		// silent - unknown message
	}
}

func (a *Agent) handleTunnel(payload *protocol.TunnelOpenPayload) {
	conn, err := net.DialTimeout("tcp", payload.Target, 10*time.Second)
	if err != nil {
		log.Printf("tunnel dial error: %v", err)
		return
	}
	defer conn.Close()

	ch := make(chan []byte, 64)
	a.tunnelMu.Lock()
	if a.tunnelStreams == nil {
		a.tunnelStreams = make(map[string]chan []byte)
	}
	a.tunnelStreams[payload.TunnelID] = ch
	a.tunnelMu.Unlock()

	defer func() {
		a.tunnelMu.Lock()
		delete(a.tunnelStreams, payload.TunnelID)
		a.tunnelMu.Unlock()
	}()

	// Send ready signal to mother
	readyMsg := protocol.NewMessage(protocol.TypeTunnelReady, protocol.TunnelOpenPayload{
		TunnelID: payload.TunnelID,
	})
	a.send(readyMsg)

	// Read from target -> send to mother
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			data := make([]byte, n)
			copy(data, buf[:n])
			msg := protocol.NewMessage(protocol.TypeTunnelData, protocol.TunnelDataPayload{
				TunnelID: payload.TunnelID,
				Data:     data,
			})
			if a.send(msg) != nil {
				return
			}
		}
	}()

	// Read from mother channel -> write to target
	for {
		select {
		case data, ok := <-ch:
			if !ok {
				return
			}
			if _, err := conn.Write(data); err != nil {
				return
			}
		}
	}
}

func (a *Agent) executeTask(task *protocol.TaskPayload) {
	// silent - executing

	start := time.Now()
	var cancel context.CancelFunc
	ctx := context.Background()
	if task.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(task.Timeout)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, task.Command, task.Args...)
	if task.Env != nil {
		cmd.Env = os.Environ()
		for k, v := range task.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}

	finish := func(exitCode int, stdout, stderr string, stdoutTruncated, stderrTruncated bool) {
		result := protocol.NewMessage(protocol.TypeTaskResult, protocol.TaskResultPayload{
			TaskID:          task.TaskID,
			ExitCode:        exitCode,
			Stdout:          stdout,
			Stderr:          stderr,
			Duration:        time.Since(start).Milliseconds(),
			StdoutTruncated: stdoutTruncated,
			StderrTruncated: stderrTruncated,
		})
		if err := a.send(result); err != nil {
			// silent
		}
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		finish(-1, "", err.Error(), false, false)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		finish(-1, "", err.Error(), false, false)
		return
	}

	outBuf := newLimitedBuffer(maxTaskOutputBytes)
	errBuf := newLimitedBuffer(maxTaskOutputBytes)

	if err := cmd.Start(); err != nil {
		finish(-1, "", err.Error(), false, false)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(outBuf, stdout)
	}()
	go func() {
		defer wg.Done()
		io.Copy(errBuf, stderr)
	}()

	err = cmd.Wait()
	wg.Wait()

	exitCode := 0
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			exitCode = -1
			if errBuf.Len() > 0 {
				errBuf.Write([]byte("\n"))
			}
			errBuf.Write([]byte("task timed out"))
		} else if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
			if errBuf.Len() > 0 {
				errBuf.Write([]byte("\n"))
			}
			errBuf.Write([]byte(err.Error()))
		}
	}

	finish(exitCode, outBuf.String(), errBuf.String(), outBuf.Truncated(), errBuf.Truncated())
}

func (a *Agent) send(msg protocol.Message) error {
	if a.httpMode {
		return a.httpPostMessages(context.Background(), []protocol.Message{msg})
	}
	data, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}
	a.mu.Lock()
	if a.conn == nil {
		a.mu.Unlock()
		return fmt.Errorf("websocket not connected")
	}
	conn := a.conn
	if err := conn.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
		a.mu.Unlock()
		a.closeConn(conn)
		return err
	}
	err = conn.WriteMessage(websocket.BinaryMessage, data)
	a.mu.Unlock()
	if err != nil {
		a.closeConn(conn)
	}
	return err
}

func (a *Agent) Close() {
	close(a.stop)
	a.closeCurrentConn()
	a.stopSSHTunnel()
}

func (a *Agent) closeCurrentConn() {
	a.mu.Lock()
	conn := a.conn
	a.conn = nil
	a.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

func (a *Agent) closeConn(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	a.mu.Lock()
	if a.conn == conn {
		a.conn = nil
	}
	a.mu.Unlock()
	conn.Close()
}

// startSSHTunnel runs: ssh -N -L tunnelPort:localhost:10300 user@host
func (a *Agent) startSSHTunnel() error {
	if a.SSHHost == "" {
		return nil
	}
	args := []string{
		"-N",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ServerAliveInterval=30",
		"-o", "ExitOnForwardFailure=yes",
		"-p", a.SSHPort,
		"-L", a.TunnelPort + ":localhost:10300",
	}
	if a.SSHKey != "" {
		args = append(args, "-i", a.SSHKey)
	}
	args = append(args, a.SSHUser+"@"+a.SSHHost)

	cmd := exec.Command("ssh", args...)
	if a.SSHPass != "" {
		cmd.Stdin = stringsReader(a.SSHPass + "\n")
	}
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("ssh: %w", err)
	}
	// Wait briefly for the tunnel to establish
	time.Sleep(1500 * time.Millisecond)
	// If exited immediately, tunnel failed
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return fmt.Errorf("ssh tunnel exited immediately")
	}
	a.tunnelCmd = cmd
	log.Printf("SSH tunnel established: localhost:%s → %s:10300", a.TunnelPort, a.SSHHost)
	return nil
}

func (a *Agent) stopSSHTunnel() {
	if a.tunnelCmd == nil {
		return
	}
	if a.tunnelCmd.Process != nil {
		a.tunnelCmd.Process.Kill()
	}
	a.tunnelCmd = nil
}

// replaceHostPort replaces host:port in a ws:// URL
func replaceHostPort(url, newHostPort string) string {
	// ws://host:port/path → ws://newHostPort/path
	s := url
	if i := idxStr(s, "://"); i >= 0 {
		s = s[i+3:]
		prefix := url[:i+3]
		if j := idxStr(s, "/"); j >= 0 {
			return prefix + newHostPort + s[j:]
		}
		return prefix + newHostPort
	}
	return url
}

func idxStr(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func stringsReader(s string) *stringsReaderImpl { return &stringsReaderImpl{s: s} }

type stringsReaderImpl struct {
	s string
	i int
}

func (r *stringsReaderImpl) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}

func decode(src, dst interface{}) {
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

const (
	wsReadWait         = 75 * time.Second
	wsWriteWait        = 10 * time.Second
	maxTaskOutputBytes = 4 * 1024 * 1024
)

type limitedBuffer struct {
	buf       bytes.Buffer
	max       int
	truncated bool
}

func newLimitedBuffer(max int) *limitedBuffer {
	return &limitedBuffer{max: max}
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	remaining := b.max - b.buf.Len()
	if remaining <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) > remaining {
		b.buf.Write(p[:remaining])
		b.truncated = true
		return len(p), nil
	}
	b.buf.Write(p)
	return len(p), nil
}

func (b *limitedBuffer) String() string {
	return b.buf.String()
}

func (b *limitedBuffer) Len() int {
	return b.buf.Len()
}

func (b *limitedBuffer) Truncated() bool {
	return b.truncated
}
