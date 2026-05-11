package protocol

import "time"

// Message types
const (
	TypeHeartbeat   = "heartbeat"
	TypeRegister    = "register"
	TypeRegistered  = "registered"
	TypeReport      = "report"
	TypeTask        = "task"
	TypeTaskResult  = "task_result"
	TypeTunnelOpen  = "tunnel_open"
	TypeTunnelClose = "tunnel_close"
	TypeTunnelData  = "tunnel_data"
	TypeShell       = "shell"
	TypeShellResult = "shell_result"
	TypeError       = "error"
)

// Message is the top-level envelope for all WS messages.
type Message struct {
	Type      string      `msgpack:"type"`
	ID        string      `msgpack:"id"`
	ChildID   string      `msgpack:"child_id,omitempty"`
	Timestamp int64       `msgpack:"ts"`
	Payload   interface{} `msgpack:"payload,omitempty"`
}

// RegisterPayload is sent by child on first connect.
type RegisterPayload struct {
	Hostname string `msgpack:"hostname"`
	OS       string `msgpack:"os"`
	Arch     string `msgpack:"arch"`
	Version  string `msgpack:"version"`
}

// RegisteredPayload is sent by mother after successful registration.
type RegisteredPayload struct {
	ChildID    string `msgpack:"child_id"`
	HeartbeatS int    `msgpack:"heartbeat_s"`
}

// HeartbeatPayload is sent periodically from child to mother.
type HeartbeatPayload struct {
	Seq int64 `msgpack:"seq"`
}

// ReportPayload is system status report from child.
type ReportPayload struct {
	CPUPercent    float64 `msgpack:"cpu"`
	MemUsedBytes  uint64  `msgpack:"mem_used"`
	MemTotalBytes uint64  `msgpack:"mem_total"`
	DiskUsedBytes uint64  `msgpack:"disk_used"`
	DiskTotalBytes uint64 `msgpack:"disk_total"`
	NetRxBytes    uint64  `msgpack:"net_rx"`
	NetTxBytes    uint64  `msgpack:"net_tx"`
	UptimeSeconds int64   `msgpack:"uptime"`
}

// TaskPayload is a command from mother to child.
type TaskPayload struct {
	TaskID  string            `msgpack:"task_id"`
	Command string            `msgpack:"command"`
	Args    []string          `msgpack:"args,omitempty"`
	Env     map[string]string `msgpack:"env,omitempty"`
	Timeout int               `msgpack:"timeout"` // seconds, 0 = no limit
}

// TaskResultPayload is the result back to mother.
type TaskResultPayload struct {
	TaskID   string `msgpack:"task_id"`
	ExitCode int    `msgpack:"exit_code"`
	Stdout   string `msgpack:"stdout"`
	Stderr   string `msgpack:"stderr"`
	Duration int64  `msgpack:"duration_ms"`
}

// TunnelOpenPayload requests a tunnel from child.
type TunnelOpenPayload struct {
	TunnelID string `msgpack:"tunnel_id"`
	Target   string `msgpack:"target"` // host:port
}

// TunnelDataPayload carries tunnel traffic.
type TunnelDataPayload struct {
	TunnelID string `msgpack:"tunnel_id"`
	Data     []byte `msgpack:"data"`
}

// ErrorPayload for error responses.
type ErrorPayload struct {
	Code    int    `msgpack:"code"`
	Message string `msgpack:"message"`
}

// NewMessage creates a new message envelope.
func NewMessage(typ string, payload interface{}) Message {
	return Message{
		Type:      typ,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
	}
}