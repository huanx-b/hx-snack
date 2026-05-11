# hxの偷吃

Distributed C2 Framework — **Mother-Child WebSocket architecture** for monitoring, tunneling, and task execution across globally deployed nodes.

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
- **WebSocket tunnels** — TCP port forwarding through WS (Mother port → Child internal service)
- **WebUI** — Dark-red cinematic dashboard with live status
- **Auto-deploy** — One-command child node installation
- **Auto-reconnect** — Exponential backoff on connection loss

## Quick Start

### Mother (your main server)

```bash
./mother-linux-amd64 -port 8080 -key my-secret-key
```

Open `http://localhost:8080` for the WebUI.

### Child (any VPS)

```bash
curl -sL https://github.com/huanxherta/hx-snack/releases/latest/download/install.sh | bash -s -- \
  --mother wss://your-mother.com/ws \
  --key my-secret-key
```

## API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/children` | GET | List all connected children |
| `/api/tasks` | POST | Send task to child |
| `/api/tasks` | GET | List task results |
| `/api/tunnels` | POST | Create tunnel |
| `/api/tunnels` | GET | List active tunnels |
| `/api/stats` | GET | System stats |
| `/api/events` | GET | SSE event stream |
| `/ws` | WS | Child connection endpoint |

## Build from Source

```bash
git clone https://github.com/huanxherta/hx-snack
cd hx-snack
go build -o mother ./cmd/mother/
go build -o child ./cmd/child/
```

## License

GPL-3.0

## Author

huanxherta