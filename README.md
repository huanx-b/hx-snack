# hx-snack

自用内网穿透与节点管理工具。一个主控端负责 Web 面板、API、隧道入口和 HTTP 代理，多个节点主动连回主控端，用于在自己的服务器、开发机、家庭网络或临时环境之间做轻量转发和运维管理。

> 一个主控端，多个自用节点。

## 项目定位

- **内网穿透**：在主控端开放端口，通过在线节点转发到节点侧可访问的服务。
- **HTTP 代理**：通过 `/p/...` 把请求从主控端转发到节点侧访问目标。
- **节点监控**：查看节点 CPU、内存、网络、在线状态和运行时间。
- **远程运维命令**：通过 Web 面板或 API 对指定节点执行维护命令。
- **自动重连**：节点断线后自动回连；主控端按来源 IP 保持节点 ID 稳定。
- **单 IP 单节点**：同一个来源 IP 只保留一个节点，重连会覆盖旧连接。

## 架构

```text
┌─────────────────────────────────────┐
│              主控端 Mother          │
│  WebUI / API / WebSocket / Tunnel   │
└──────────────────┬──────────────────┘
                   │ WebSocket 或 HTTP 长轮询
       ┌───────────┼───────────┐
       │           │           │
┌──────▼──────┐ ┌──▼───────┐ ┌─▼────────┐
│ 节点 A      │ │ 节点 B   │ │ 节点 C   │
│ 家庭网络    │ │ VPS      │ │ 开发机   │
└─────────────┘ └──────────┘ └──────────┘
```

## 快速开始

### 启动主控端

```bash
go build -o mother ./cmd/mother/
./mother -port 8080 -key my-secret-key
```

打开：

```text
http://localhost:8080
```

默认管理后台：

```text
http://localhost:8080/admin
```

默认账号密码：

```text
huanx / REDACTED1
```

### 启动节点

```bash
go build -o child ./cmd/child/
./child -host ws://主控端地址:8080/api/stream -key my-secret-key
```

节点会主动连接主控端。主控端按照节点来源 IP 生成稳定 ID，例如：

```text
ip_203_0_113_10
```

同一个 IP 的节点重连后仍使用同一个 ID。

## TCP 隧道

创建一个从主控端端口到节点侧服务的转发：

```bash
curl -X POST http://localhost:8080/api/tunnels \
  -H "Content-Type: application/json" \
  -d '{"child_id":"ip_203_0_113_10","target":"127.0.0.1:22","listen_port":10022}'
```

如果不指定 `child_id`，主控端会把当前在线节点加入同一端口的转发池：

```bash
curl -X POST http://localhost:8080/api/tunnels \
  -H "Content-Type: application/json" \
  -d '{"target":"127.0.0.1:8080","listen_port":18080}'
```

## HTTP 代理

通过在线节点访问 HTTP/HTTPS 目标：

```bash
curl http://localhost:8080/p/http://example.com/
curl http://localhost:8080/p/https://api.example.com/v1/status
```

也支持简写，默认按 HTTP 处理：

```bash
curl http://localhost:8080/p/example.com/
```

## API 概览

| Endpoint | Method | 说明 |
| --- | --- | --- |
| `/api/children` | GET | 查看在线节点 |
| `/api/children?id=...` | DELETE | 断开指定节点 |
| `/api/tasks` | POST/GET | 下发命令 / 查看任务 |
| `/api/tasks/{id}` | GET | 查看单个任务结果 |
| `/api/tunnels` | POST/GET | 创建 / 查看隧道 |
| `/api/tunnels/{id}` | DELETE | 关闭隧道 |
| `/api/stats` | GET | 查看统计信息 |
| `/api/events` | GET | WebUI 事件流 |
| `/ws` | WS | 节点 WebSocket 入口 |
| `/api/stream` | WS | 节点 WebSocket 入口 |
| `/api/http/*` | HTTP | 节点 HTTP 长轮询入口 |
| `/p/...` | ANY | 通过节点转发 HTTP 请求 |

### 下发命令

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"child_id":"ip_203_0_113_10","command":"uname","args":["-a"],"timeout":30}'
```

任务结果包含 stdout、stderr、退出码和耗时。节点会并发读取 stdout/stderr，避免输出较多时互相阻塞；单路输出超过限制时会返回截断标记：

```json
{
  "stdout_truncated": true,
  "stderr_truncated": false
}
```

## 构建发布

```bash
scripts/build.sh dev
```

默认构建：

- `dist/mother-linux-amd64`
- `dist/mother-linux-arm64`
- `dist/child-linux-amd64`
- `dist/child-linux-arm64`

## 目录结构

```text
cmd/mother/              主控端入口
cmd/child/               节点入口
internal/mother/         主控端 Hub、API、隧道、代理
internal/child/          节点连接、监控、命令执行
internal/protocol/       消息协议定义
internal/mother/web/     内嵌 WebUI 和下载资源
docs/                    协议说明
scripts/                 构建和安装脚本
```

## 当前约定

- 节点 ID 由来源 IP 派生：`ip_1_2_3_4`。
- 同 IP 新连接会覆盖旧连接。
- WebSocket 和 HTTP 长轮询使用相同 ID 策略。
- 任务输出默认按 stdout/stderr 各 4 MiB 截断。
- 所有运行状态目前保存在内存中，主控端重启后需要重新连接节点。

## License

GPL-3.0
