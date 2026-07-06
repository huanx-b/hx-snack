---
name: hx-snack-child-protocol
description: hx-snack child node protocol spec — language-agnostic reference for implementing child agents in Rust, Node.js, C, or any language. Covers msgpack wire format, WebSocket handshake, PSK auth, message types, heartbeat, task execution, tunnels, and all known pitfalls.
---

# hx-snack Child Protocol — Language-Agnostic Spec

Use this when implementing a child node from scratch in ANY language (Rust, Node.js, C, Zig, Java, etc.). Covers every byte on the wire.

**Reference implementations:** Go (`internal/child/`) and Python (`child.py`, pure stdlib, zero deps).

## Quick Start Checklist

1. WebSocket connect to `ws://<mother>:10300/api/stream?key=<psk>`
2. Send `register` message with hostname/os/arch/version
3. Start heartbeat loop (8-25s random interval)
4. Start monitor loop (15s interval, read /proc on Linux)
5. Read loop: dispatch `task`, `tunnel_open`, `tunnel_data`
6. Auto-reconnect with exponential backoff (1s → 60s max)

## 1. Transport & Auth

### Endpoint
```
ws://<mother_host>:10300/api/stream?key=<psk>
```

- Path MUST be `/api/stream` (also works at `/ws` — same handler)
- PSK sent as query param `?key=REMOVED-KEY`
- No other auth headers needed
- WebSocket subprotocol: none

### WebSocket Handshake
Standard RFC 6455:
- `Sec-WebSocket-Key`: 16 random bytes base64
- Verify `Sec-WebSocket-Accept` = `base64(sha1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))`
- Status must be 101

### Frame Rules
- Client→Server: **MUST mask** every frame (MASK bit=1, 4 random mask bytes)
- Server→Client: unmasked (MASK bit=0)
- Opcode: `0x2` (binary) for all data
- Control frames: auto-pong on ping (opcode `0x9`)
- **⚠️ Frame header byte order**: Payload length fields (16-bit at 126, 64-bit at 127) MUST be big-endian. On little-endian CPUs (x86, ARM), forgetting this → `bad MASK` errors from mother.

### Frame header format
```
Byte 0: [FIN(1)|RSV(3)|OPCODE(4)]
Byte 1: [MASK(1)|PAYLOAD_LEN(7)]
If PAYLOAD_LEN=126: +2 bytes uint16 big-endian
If PAYLOAD_LEN=127: +8 bytes uint64 big-endian
If MASK=1: +4 bytes mask key
Then: payload bytes (XOR'd with mask key if MASK=1)
```

## 2. Wire Format: msgpack

All messages are msgpack-encoded binary frames. Go mother uses `vmihailenco/msgpack/v5`.

### Type Mapping (CRITICAL for compatibility)

| Go type | msgpack byte | Notes |
|---------|-------------|-------|
| `string` | fixstr (`0xa0-0xbf`) or str8/16/32 (`0xd9-0xdb`) | UTF-8 |
| `[]byte` | bin8/16/32 (`0xc4-0xc6`) | Raw bytes — used in tunnel data |
| `int`/`int64`/`uint64` | fixint (`0x00-0x7f`) or int/uint 8/16/32/64 (`0xcc-0xd3`) | |
| `float64` | float64 (`0xcb`) | Used for CPU % |
| `bool` | false (`0xc2`) / true (`0xc3`) | |
| `nil` | nil (`0xc0`) | |
| `map[string]interface{}` | fixmap (`0x80-0x8f`) or map16/32 (`0xde-0xdf`) | All messages are maps |
| `[]interface{}` | fixarray (`0x90-0x9f`) or array16/32 (`0xdc-0xdd`) | Task args |

### Key Pitfalls
- **`[]byte` MUST be msgpack `bin`, NOT `str`**: Go serializes `[]byte` as bin family. If your msgpack lib maps `[]byte` to str, tunnel data will corrupt.
- **Structs map to fixmap**: Go structs become msgpack maps with string keys matching json tags.
- **Missing fields**: Both sides handle absent keys gracefully — `payload` may be omitted when nil.

## 3. Message Types

Every message is a msgpack map: `{"type": "<string>", "ts": <int64_ms>, "payload": {...}}` (ts optional, payload optional).

### Child → Mother (sent by you)

| type | payload | When |
|------|---------|------|
| `"register"` | `{hostname, os, arch, version}` | Immediately after WebSocket connect |
| `"heartbeat"` | `{seq: int}` | Every 8-25s (randomized) |
| `"report"` | `{cpu, mem_used, mem_total, disk_used, disk_total, net_rx, net_tx, uptime}` | Every 15s |
| `"task_result"` | `{task_id, exit_code, stdout, stderr, duration_ms}` | After executing a task |
| `"tunnel_ready"` | `{tunnel_id}` | After dialing tunnel target successfully |
| `"tunnel_data"` | `{tunnel_id, data: bytes}` | Bidirectional — you send & receive this |

### Mother → Child (received by you)

| type | payload | Action |
|------|---------|--------|
| `"registered"` | `{child_id, heartbeat_s: 5}` | Store child_id |
| `"heartbeat"` | `{}` | Echo response — ignore or log |
| `"task"` | `{task_id, command, args, env, timeout}` | Execute command, send task_result |
| `"tunnel_open"` | `{tunnel_id, target}` | Dial target, send tunnel_ready, start bidirectional relay |
| `"tunnel_data"` | `{tunnel_id, data: bytes}` | Write data to connected tunnel socket |
| `"tunnel_close"` | `{tunnel_id}` | Close tunnel socket |
| `"error"` | `{message}` | Log and continue |

### Register Payload
```json
{
  "hostname": "my-machine",
  "os": "linux",        // runtime.GOOS
  "arch": "amd64",       // runtime.GOARCH (or "python" for Python child)
  "version": "dev"       // arbitrary string
}
```

### Report Payload
```json
{
  "cpu": 12.5,           // float64 — CPU usage percent
  "mem_used": 929000000, // uint64 — bytes used
  "mem_total": 3860000000,
  "disk_used": 15000000000,
  "disk_total": 50000000000,
  "net_rx": 5200000000,  // uint64 — cumulative bytes
  "net_tx": 100000000,
  "uptime": 1209600      // int64 — seconds
}
```

### Task Payload (from mother)
```json
{
  "task_id": "abc123...",
  "command": "curl",      // or "sh" / "python3" / etc
  "args": ["-s", "http://example.com"],
  "env": {"KEY": "val"},  // optional extra env vars
  "timeout": 30           // seconds, 0 = no timeout
}
```

### Task Result Payload (to mother)
```json
{
  "task_id": "abc123...",
  "exit_code": 0,
  "stdout": "output text",
  "stderr": "",
  "duration_ms": 475
}
```

## 4. Connection Lifecycle

```
1. WebSocket connect → /api/stream?key=<psk>
2. Send "register" → receive "registered" (with child_id)
3. Start 2 background loops:
   a. heartbeat: every 8-25s, send {"type":"heartbeat","payload":{"seq":N}}
   b. monitor: every 15s, send {"type":"report","payload":{...metrics...}}
4. Read loop: dispatch incoming messages by type
5. On disconnect → exponential backoff: 1s, 2s, 4s, 8s, ... max 60s
6. On successful reconnect → reset backoff to 1s
```

### Reconnect Rules
- Start at 1 second
- Double each failure: 1 → 2 → 4 → 8 → 16 → 32 → 60 (cap)
- On ANY successful connect → reset to 1s immediately
- Mother restart = instant reconnect (child detects TCP close)

## 5. Tunnel (Bidirectional TCP Forwarding)

### Tunnel Open Flow
```
Mother                          Child
  |                               |
  |--- tunnel_open {id, target}-->|
  |                               |--- dial target TCP
  |<-- tunnel_ready {id} ---------|
  |                               |
  |<== tunnel_data {id, data} ===>|  (bidirectional)
  |                               |
```

### Implementation Steps
1. Receive `tunnel_open` with `{tunnel_id, target}` (target = "host:port")
2. `net.Dial("tcp", target)`
3. Send `tunnel_ready` with `{tunnel_id}`
4. Spawn two goroutines/threads:
   - **Target→Mother**: read from target socket → send `tunnel_data {tunnel_id, data: bytes}`
   - **Mother→Target**: receive `tunnel_data` → write to target socket
5. On either side EOF/error → close both, send `tunnel_close`
6. On `tunnel_close` from mother → close target socket

### Key Detail
- `data` field in `tunnel_data` is msgpack **bin** (raw bytes), NOT string
- Max chunk size: 32KB recommended
- Multiple tunnels can be active simultaneously

## 6. Heartbeat Design

```
Child sends:  {"type":"heartbeat","payload":{"seq":0}}
Mother echoes: {"type":"heartbeat","payload":{}}  // seq NOT echoed
```

- Interval: random 8-25 seconds to avoid synchronized reconnect/report bursts
- Mother config says `heartbeat_s: 5` but child should use 8-25s range
- No timeout detection needed on child side — TCP disconnect handles it

## 7. Monitoring (Linux /proc)

No external libraries needed. All from /proc:

| Metric | Source | How |
|--------|--------|-----|
| CPU% | `/proc/stat` line 1 | `(total - idle) / total * 100` |
| Memory | `/proc/meminfo` | `MemTotal - MemAvailable` |
| Disk | `statvfs("/")` | `f_blocks * f_frsize` |
| Network | `/proc/net/dev` | Sum rx/tx across all interfaces |
| Uptime | `/proc/uptime` | First field, truncate to int |

## 8. Known Pitfalls (ALL languages)

### 🔴 CRITICAL: Byte Order in WebSocket Frame Header
Payload length fields in the frame header MUST be big-endian. x86 and ARM are little-endian by default.
- Python: `struct.pack('>H', n)` not `struct.pack('H', n)`
- Rust: `u16::to_be_bytes()` not `u16::to_le_bytes()`
- C: `htons(n)` not raw `n`
- Node.js: `buf.writeUInt16BE(n)` not `buf.writeUInt16LE(n)`
Symptom of error: mother logs `websocket: bad MASK` and disconnects.

### 🔴 CRITICAL: msgpack `[]byte` vs `string`
Go serializes `[]byte` as msgpack **bin** (0xc4-0xc6), not str (0xa0-0xbf). Tunnel data IS raw bytes. If your msgpack library maps bytes to strings, tunnel forwarding will corrupt binary data. Check your library's type mapping.

### 🟡 PSK in URL
Don't forget the `?key=<psk>` query parameter — mother returns 401 without it.

### 🟡 Path must be `/api/stream` (or `/ws`)
Root path `/` returns HTML dashboard (200), not WebSocket upgrade (101). Your client must check for 101 status.

### 🟡 Frame MUST be binary opcode (0x2)
Mother expects msgpack binary frames. Text frames (0x1) will fail deserialization.

### 🟡 Client frames MUST have MASK bit set
RFC 6455 requires client→server frames to be masked. Server will reject unmasked frames.

### 🟡 Reconnect backoff cap
Don't hammer the mother — 60s max backoff. Mother restart storms can fill logs with millions of `bad MASK` entries in hours.

## 9. Reference Implementations

- **Go**: `/root/hx-snack/internal/child/agent.go` — production, uses gorilla/websocket + vmihailenco/msgpack
- **Python**: `/root/hx-snack/child.py` — pure stdlib, zero deps, hand-rolled msgpack + WebSocket

Both are self-contained single files. Use the Python version as the simplest reference — it has no hidden dependencies or magic.

## 10. SSH Tunnel Mode (Optional)

For environments where port 10300 is blocked but SSH (port 22) is open:

```
ssh -N -L 10399:localhost:10300 user@mother_host
```

Then connect WebSocket to `ws://localhost:10399/api/stream?key=<psk>`. The SSH tunnel transparently forwards to mother's port 10300.

Both Go and Python children support this natively — just set `SSH_TUNNEL=True` in config.

## 11. Quick Test Script

Before writing a full child, verify connectivity with this minimal test:

```python
# Minimal test — just connect, register, read one message
import asyncio, websockets, msgpack

async def test():
    async with websockets.connect("ws://HOST:10300/api/stream?key=PSK") as ws:
        # Register
        await ws.send(msgpack.packb({"type":"register","payload":{"hostname":"test","os":"linux","arch":"amd64","version":"0"}}))
        # Read response
        raw = await ws.recv()
        print(msgpack.unpackb(raw))  # Should show "registered" with child_id

asyncio.run(test())
```

If this fails, check: host reachable, PSK correct, WebSocket library masks frames correctly.

## 12. Deployment & Daemonization

A child node MUST run persistently in the background and survive terminal logout, SSH disconnect, and process crashes.

### Option A: Self-Daemonizing (Recommended)

The child SHOULD daemonize itself at startup, avoiding external tool dependencies:

1. **Double-fork**: `fork()` → parent exits → child `fork()` → grandchild is the daemon
2. **setsid()**: Detach from controlling terminal
3. **chdir("/")**: Avoid holding mount points
4. **Close stdio**: Redirect stdin/stdout/stderr to `/dev/null`
5. **Signal handlers**: `SIGTERM`/`SIGINT` → graceful shutdown (close WS, kill SSH tunnel)

The Go reference implementation (`cmd/child/main.go`) does NOT self-daemonize — it relies on external tools. A self-daemonizing mode is a TODO.

### Option B: External Process Supervisor

If the child doesn't self-daemonize, use one of:

| Tool | Command | Notes |
|------|---------|-------|
| **systemd** | `systemctl enable --now child` | Best for servers — auto-restart, logging |
| **nohup** | `nohup ./child &>/dev/null &` | Simplest, survives SIGHUP only |
| **screen** | `screen -dmS child ./child` | Detachable terminal, reattach with `screen -r` |
| **tmux** | `tmux new -d -s child './child'` | Same as screen, modern alternative |
| **supervisord** | `[program:child]` config | Process manager with web UI |

### Option C: Watchdog Wrapper (Crash Recovery)

If the language runtime is unstable (e.g., Python with memory leaks), wrap in a restart loop:

```bash
#!/bin/bash
# watchdog.sh — restart child on crash with backoff
BACKOFF=1
while true; do
    ./child
    echo "[watchdog] child exited, restarting in ${BACKOFF}s"
    sleep $BACKOFF
    BACKOFF=$(( BACKOFF < 60 ? BACKOFF * 2 : 60 ))
done
```

**Note**: The child already has internal reconnect logic for WebSocket drops. The watchdog is for PROCESS crashes (segfault, OOM, uncaught exception). Don't confuse the two — if WebSocket drops but process stays alive, the child's own backoff handles it.

### Deployment Notes

- **Process manager**: Prefer systemd or another supervisor for auto-restart.
- **Network**: Nodes only need outbound access to the main server.
- **Logs**: Route logs to the supervisor or system journal for easier troubleshooting.
- **Identity**: The main server treats one source IP as one node.

### systemd Unit Example

```ini
[Unit]
Description=System Log Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/logcollector
Restart=always
RestartSec=10
User=nobody
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
```

Key: `Restart=always` + `RestartSec=10` gives crash recovery at the OS level. The `After=network-online.target` ensures network is up before connecting.
