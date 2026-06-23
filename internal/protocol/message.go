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
	TypeTunnelReady = "tunnel_ready"
	TypeTunnelClose = "tunnel_close"
	TypeTunnelData  = "tunnel_data"
	TypeShell       = "shell"
	TypeShellResult = "shell_result"
	TypeError       = "error"
)

// Message is the top-level envelope for all WS messages.
type Message struct {
	Type      string      `msgpack:"type" json:"type"`
	ID        string      `msgpack:"id" json:"id,omitempty"`
	ChildID   string      `msgpack:"child_id,omitempty" json:"child_id,omitempty"`
	Timestamp int64       `msgpack:"ts" json:"ts"`
	Payload   interface{} `msgpack:"payload,omitempty" json:"payload,omitempty"`
}

// RegisterPayload is sent by child on first connect.
type RegisterPayload struct {
	Hostname string `msgpack:"hostname" json:"hostname"`
	OS       string `msgpack:"os" json:"os"`
	Arch     string `msgpack:"arch" json:"arch"`
	Version  string `msgpack:"version" json:"version"`
}

// RegisteredPayload is sent by mother after successful registration.
type RegisteredPayload struct {
	ChildID    string `msgpack:"child_id" json:"child_id"`
	HeartbeatS int    `msgpack:"heartbeat_s" json:"heartbeat_s"`
}

// HeartbeatPayload is sent periodically from child to mother.
type HeartbeatPayload struct {
	Seq int64 `msgpack:"seq" json:"seq"`
}

// ReportPayload is system status report from child.
type ReportPayload struct {
	CPUPercent     float64 `msgpack:"cpu" json:"cpu"`
	MemUsedBytes   uint64  `msgpack:"mem_used" json:"mem_used"`
	MemTotalBytes  uint64  `msgpack:"mem_total" json:"mem_total"`
	DiskUsedBytes  uint64  `msgpack:"disk_used" json:"disk_used"`
	DiskTotalBytes uint64  `msgpack:"disk_total" json:"disk_total"`
	NetRxBytes     uint64  `msgpack:"net_rx" json:"net_rx"`
	NetTxBytes     uint64  `msgpack:"net_tx" json:"net_tx"`
	UptimeSeconds  int64   `msgpack:"uptime" json:"uptime"`
}

// TaskPayload is a command from mother to child.
type TaskPayload struct {
	TaskID  string            `msgpack:"task_id" json:"task_id"`
	Command string            `msgpack:"command" json:"command"`
	Args    []string          `msgpack:"args,omitempty" json:"args,omitempty"`
	Env     map[string]string `msgpack:"env,omitempty" json:"env,omitempty"`
	Timeout int               `msgpack:"timeout" json:"timeout"` // seconds, 0 = no limit
}

// TaskResultPayload is the result back to mother.
type TaskResultPayload struct {
	TaskID   string `msgpack:"task_id" json:"task_id"`
	ExitCode int    `msgpack:"exit_code" json:"exit_code"`
	Stdout   string `msgpack:"stdout" json:"stdout"`
	Stderr   string `msgpack:"stderr" json:"stderr"`
	Duration int64  `msgpack:"duration_ms" json:"duration_ms"`
}

// TunnelOpenPayload requests a tunnel from child.
type TunnelOpenPayload struct {
	TunnelID string `msgpack:"tunnel_id" json:"tunnel_id"`
	Target   string `msgpack:"target" json:"target"` // host:port
}

// TunnelDataPayload carries tunnel traffic.
type TunnelDataPayload struct {
	TunnelID string `msgpack:"tunnel_id" json:"tunnel_id"`
	Data     []byte `msgpack:"data" json:"data"`
}

// ErrorPayload for error responses.
type ErrorPayload struct {
	Code    int    `msgpack:"code" json:"code"`
	Message string `msgpack:"message" json:"message"`
}

// NewMessage creates a new message envelope.
func NewMessage(typ string, payload interface{}) Message {
	return Message{
		Type:      typ,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
	}
}
