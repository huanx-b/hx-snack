#!/usr/bin/env python3
"""
hx-snack Python Child — 纯标准库，零外部依赖
msgpack + WebSocket 全部手写实现。
"""

# ====== 硬编码配置（部署时修改这里） ======
MOTHER_URL = "ws://0.0.0.0.0.0.0:10300/api/stream"
MOTHER_KEY = "REMOVED-KEY"
# ============================================

import os
import sys
import time
import json
import struct
import socket
import random
import hashlib
import base64
import string
import signal
import threading
import subprocess
import queue

# ═══════════════════════════════════════════════
#  Minimal msgpack encoder/decoder
# ═══════════════════════════════════════════════

def _msgpack_pack(obj):
    """Encode a Python object to msgpack bytes."""
    if obj is None:
        return b'\xc0'
    if isinstance(obj, bool):
        return b'\xc3' if obj else b'\xc2'
    if isinstance(obj, int):
        if 0 <= obj <= 0x7f:
            return struct.pack('B', obj)
        if obj < 0:
            if obj >= -32:
                return struct.pack('b', obj)
            elif obj >= -128:
                return b'\xd0' + struct.pack('b', obj)
            elif obj >= -32768:
                return b'\xd1' + struct.pack('>h', obj)
            elif obj >= -2147483648:
                return b'\xd2' + struct.pack('>i', obj)
            else:
                return b'\xd3' + struct.pack('>q', obj)
        else:
            if obj <= 0xff:
                return b'\xcc' + struct.pack('B', obj)
            elif obj <= 0xffff:
                return b'\xcd' + struct.pack('>H', obj)
            elif obj <= 0xffffffff:
                return b'\xce' + struct.pack('>I', obj)
            else:
                return b'\xcf' + struct.pack('>Q', obj)
    if isinstance(obj, float):
        return b'\xcb' + struct.pack('>d', obj)
    if isinstance(obj, str):
        data = obj.encode('utf-8')
        n = len(data)
        if n <= 31:
            return struct.pack('B', 0xa0 | n) + data
        elif n <= 0xff:
            return b'\xd9' + struct.pack('B', n) + data
        elif n <= 0xffff:
            return b'\xda' + struct.pack('>H', n) + data
        else:
            return b'\xdb' + struct.pack('>I', n) + data
    if isinstance(obj, bytes):
        n = len(obj)
        if n <= 0xff:
            return b'\xc4' + struct.pack('B', n) + obj
        elif n <= 0xffff:
            return b'\xc5' + struct.pack('>H', n) + obj
        else:
            return b'\xc6' + struct.pack('>I', n) + obj
    if isinstance(obj, (list, tuple)):
        n = len(obj)
        if n <= 15:
            head = struct.pack('B', 0x90 | n)
        elif n <= 0xffff:
            head = b'\xdc' + struct.pack('>H', n)
        else:
            head = b'\xdd' + struct.pack('>I', n)
        return head + b''.join(_msgpack_pack(v) for v in obj)
    if isinstance(obj, dict):
        n = len(obj)
        if n <= 15:
            head = struct.pack('B', 0x80 | n)
        elif n <= 0xffff:
            head = b'\xde' + struct.pack('>H', n)
        else:
            head = b'\xdf' + struct.pack('>I', n)
        parts = []
        for k, v in obj.items():
            parts.append(_msgpack_pack(k))
            parts.append(_msgpack_pack(v))
        return head + b''.join(parts)
    # fallback: try to convert
    raise TypeError(f"msgpack: cannot encode {type(obj)}")


class _Unpacker:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def _read(self, n):
        b = self.data[self.pos:self.pos + n]
        self.pos += n
        return b

    def _read_byte(self):
        return self.data[self.pos]

    def unpack(self):
        if self.pos >= len(self.data):
            raise EOFError
        b = self._read_byte()
        # positive fixint
        if b <= 0x7f:
            self.pos += 1
            return b
        # fixmap
        if 0x80 <= b <= 0x8f:
            return self._unpack_map(b & 0x0f)
        # fixarray
        if 0x90 <= b <= 0x9f:
            return self._unpack_array(b & 0x0f)
        # fixstr
        if 0xa0 <= b <= 0xbf:
            return self._unpack_str(b & 0x1f)
        # nil
        if b == 0xc0:
            self.pos += 1
            return None
        # false
        if b == 0xc2:
            self.pos += 1
            return False
        # true
        if b == 0xc3:
            self.pos += 1
            return True
        # bin 8/16/32
        if b == 0xc4:
            self.pos += 1
            n = self._read(1)[0]
            return self._read_bytes(n)
        if b == 0xc5:
            self.pos += 1
            n = struct.unpack('>H', self._read(2))[0]
            return self._read_bytes(n)
        if b == 0xc6:
            self.pos += 1
            n = struct.unpack('>I', self._read(4))[0]
            return self._read_bytes(n)
        # uint
        if b == 0xcc:
            self.pos += 1
            return self._read(1)[0]
        if b == 0xcd:
            self.pos += 1
            return struct.unpack('>H', self._read(2))[0]
        if b == 0xce:
            self.pos += 1
            return struct.unpack('>I', self._read(4))[0]
        if b == 0xcf:
            self.pos += 1
            return struct.unpack('>Q', self._read(8))[0]
        # int
        if b == 0xd0:
            self.pos += 1
            return struct.unpack('b', self._read(1))[0]
        if b == 0xd1:
            self.pos += 1
            return struct.unpack('>h', self._read(2))[0]
        if b == 0xd2:
            self.pos += 1
            return struct.unpack('>i', self._read(4))[0]
        if b == 0xd3:
            self.pos += 1
            return struct.unpack('>q', self._read(8))[0]
        # str 8/16/32
        if b == 0xd9:
            self.pos += 1
            n = self._read(1)[0]
            return self._read_str(n)
        if b == 0xda:
            self.pos += 1
            n = struct.unpack('>H', self._read(2))[0]
            return self._read_str(n)
        if b == 0xdb:
            self.pos += 1
            n = struct.unpack('>I', self._read(4))[0]
            return self._read_str(n)
        # array 16/32
        if b == 0xdc:
            self.pos += 1
            n = struct.unpack('>H', self._read(2))[0]
            return self._unpack_array(n)
        if b == 0xdd:
            self.pos += 1
            n = struct.unpack('>I', self._read(4))[0]
            return self._unpack_array(n)
        # map 16/32
        if b == 0xde:
            self.pos += 1
            n = struct.unpack('>H', self._read(2))[0]
            return self._unpack_map(n)
        if b == 0xdf:
            self.pos += 1
            n = struct.unpack('>I', self._read(4))[0]
            return self._unpack_map(n)
        # float
        if b == 0xca:
            self.pos += 1
            return struct.unpack('>f', self._read(4))[0]
        if b == 0xcb:
            self.pos += 1
            return struct.unpack('>d', self._read(8))[0]
        # negative fixint
        if 0xe0 <= b <= 0xff:
            self.pos += 1
            return b - 256
        raise ValueError(f"msgpack: unknown byte 0x{b:02x} at pos {self.pos - 1}")

    def _unpack_map(self, n):
        self.pos += 1
        d = {}
        for _ in range(n):
            k = self.unpack()
            v = self.unpack()
            d[k] = v
        return d

    def _unpack_array(self, n):
        self.pos += 1
        return [self.unpack() for _ in range(n)]

    def _unpack_str(self, n):
        self.pos += 1
        return self._read(n).decode('utf-8', errors='replace')

    def _read_bytes(self, n):
        self.pos += 1
        return self._read(n)

    def _read_str_from_bytes(self, n):
        self.pos += 1
        return self._read(n).decode('utf-8', errors='replace')


def msgpack_unpack(data):
    return _Unpacker(data).unpack()


# ═══════════════════════════════════════════════
#  Minimal WebSocket client
# ═══════════════════════════════════════════════

OP_TEXT   = 0x1
OP_BINARY = 0x2
OP_CLOSE  = 0x8
OP_PING   = 0x9
OP_PONG   = 0xA


def _ws_key():
    return base64.b64encode(os.urandom(16)).decode()


def _ws_accept(key):
    sha = hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    return base64.b64encode(sha).decode()


class WebSocket:
    """Minimal WebSocket client. Call recv() for next (opcode, data)."""

    def __init__(self):
        self.sock = None
        self._recv_buf = b''

    def connect(self, url, timeout=10):
        # Parse URL
        if url.startswith('ws://'):
            host_part = url[5:]
            tls = False
        elif url.startswith('wss://'):
            host_part = url[6:]
            tls = True
        else:
            raise ValueError(f"Invalid WS URL: {url}")

        if '/' in host_part:
            host, path = host_part.split('/', 1)
            path = '/' + path
        else:
            host, path = host_part, '/'

        if ':' in host:
            hostname, port = host.rsplit(':', 1)
            port = int(port)
        else:
            hostname, port = 443 if tls else 80

        # TCP connect
        self.sock = socket.create_connection((hostname, port), timeout=timeout)

        if tls:
            import ssl
            ctx = ssl.create_default_context()
            self.sock = ctx.wrap_socket(self.sock, server_hostname=hostname)

        # Handshake
        key = _ws_key()
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {hostname}:{port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"\r\n"
        ).encode()

        self.sock.sendall(req)

        # Read response
        response = b''
        while b'\r\n\r\n' not in response:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("WebSocket handshake failed: no response")
            response += chunk

        head, body = response.split(b'\r\n\r\n', 1)
        self._recv_buf = body

        header_lines = head.decode().split('\r\n')
        status = header_lines[0]
        if '101' not in status:
            raise ConnectionError(f"WebSocket upgrade rejected: {status}")

        # Verify accept
        expected = _ws_accept(key)
        for line in header_lines[1:]:
            if line.lower().startswith('sec-websocket-accept:'):
                got = line.split(':', 1)[1].strip()
                if got != expected:
                    raise ConnectionError("WebSocket accept mismatch")

    def _read_exact(self, n):
        while len(self._recv_buf) < n:
            chunk = self.sock.recv(max(4096, n - len(self._recv_buf)))
            if not chunk:
                raise ConnectionError("WebSocket connection closed")
            self._recv_buf += chunk
        data = self._recv_buf[:n]
        self._recv_buf = self._recv_buf[n:]
        return data

    def recv(self, timeout=None):
        """Returns (opcode, data) or raises on close/error."""
        if timeout:
            self.sock.settimeout(timeout)

        # Read frame header
        hdr = self._read_exact(2)
        b0, b1 = hdr[0], hdr[1]
        fin = (b0 >> 7) & 1
        opcode = b0 & 0x0f
        masked = (b1 >> 7) & 1

        plen = b1 & 0x7f
        if plen == 126:
            plen = struct.unpack('>H', self._read_exact(2))[0]
        elif plen == 127:
            plen = struct.unpack('>Q', self._read_exact(8))[0]

        # Read payload (unmasked from server)
        payload = self._read_exact(plen) if plen > 0 else b''

        # Handle control frames inline
        if opcode == OP_CLOSE:
            raise ConnectionError("WebSocket closed by server")
        if opcode == OP_PING:
            # Auto pong
            mask_key = os.urandom(4)
            masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
            frame = bytes([0x8A])  # FIN + PONG
            if len(payload) < 126:
                frame += bytes([0x80 | len(payload)])
            elif len(payload) < 65536:
                frame += bytes([0x80 | 126]) + struct.pack('>H', len(payload))
            else:
                frame += bytes([0x80 | 127]) + struct.pack('>Q', len(payload))
            frame += mask_key + masked_payload
            self.sock.sendall(frame)
            return self.recv(timeout)  # recurse for next frame

        return opcode, payload

    def send(self, data, opcode=OP_BINARY):
        """Send a frame (client must mask)."""
        mask_key = os.urandom(4)
        masked = bytes(data[i] ^ mask_key[i % 4] for i in range(len(data)))

        b0 = 0x80 | opcode  # FIN + opcode
        n = len(data)
        if n < 126:
            frame = struct.pack('BB', b0, 0x80 | n)
        elif n < 65536:
            frame = struct.pack('>BBH', b0, 0x80 | 126, n)
        else:
            frame = struct.pack('>BBQ', b0, 0x80 | 127, n)

        frame += mask_key + masked
        self.sock.sendall(frame)

    def send_ping(self, data=b''):
        self.send(data, OP_PING)

    def close(self):
        try:
            self.send(b'', OP_CLOSE)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════
#  Protocol helpers
# ═══════════════════════════════════════════════

TYPE_HEARTBEAT    = "heartbeat"
TYPE_REGISTER     = "register"
TYPE_REGISTERED   = "registered"
TYPE_REPORT       = "report"
TYPE_TASK         = "task"
TYPE_TASK_RESULT  = "task_result"
TYPE_TUNNEL_OPEN  = "tunnel_open"
TYPE_TUNNEL_READY = "tunnel_ready"
TYPE_TUNNEL_CLOSE = "tunnel_close"
TYPE_TUNNEL_DATA  = "tunnel_data"
TYPE_ERROR        = "error"


def now_ms():
    return int(time.time() * 1000)


def make_msg(msg_type, payload=None):
    msg = {"type": msg_type, "ts": now_ms()}
    if payload is not None:
        msg["payload"] = payload
    return msg


# ═══════════════════════════════════════════════
#  System monitor (no psutil needed on Linux)
# ═══════════════════════════════════════════════

def _read_file(path):
    try:
        with open(path, 'r') as f:
            return f.read().strip()
    except Exception:
        return None


def _read_proc_net():
    """Read /proc/net/dev for total rx/tx bytes."""
    try:
        with open('/proc/net/dev', 'r') as f:
            lines = f.readlines()[2:]  # skip headers
        rx = tx = 0
        for line in lines:
            parts = line.split()
            if len(parts) >= 10:
                rx += int(parts[1])
                tx += int(parts[9])
        return rx, tx
    except Exception:
        return 0, 0


def collect_report():
    r = {}
    # CPU — via /proc/stat
    try:
        with open('/proc/stat', 'r') as f:
            cpu_line = f.readline()
            parts = cpu_line.split()
            if len(parts) >= 5:
                total = sum(int(x) for x in parts[1:])
                idle = int(parts[4])
                # rough: just use raw values, not delta
                r["cpu"] = round((total - idle) / max(total, 1) * 100, 1)
    except Exception:
        r["cpu"] = 0.0

    # Memory — via /proc/meminfo
    mem_total = mem_avail = 0
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                if line.startswith('MemTotal:'):
                    mem_total = int(line.split()[1]) * 1024
                elif line.startswith('MemAvailable:'):
                    mem_avail = int(line.split()[1]) * 1024
    except Exception:
        pass
    r["mem_used"] = mem_total - mem_avail if mem_total else 0
    r["mem_total"] = mem_total

    # Disk — statvfs
    try:
        st = os.statvfs('/')
        r["disk_total"] = st.f_frsize * st.f_blocks
        r["disk_used"] = st.f_frsize * (st.f_blocks - st.f_bfree)
    except Exception:
        r["disk_total"] = 0
        r["disk_used"] = 0

    # Network
    rx, tx = _read_proc_net()
    r["net_rx"] = rx
    r["net_tx"] = tx

    # Uptime
    try:
        with open('/proc/uptime', 'r') as f:
            r["uptime"] = int(float(f.readline().split()[0]))
    except Exception:
        r["uptime"] = 0

    return r


# ═══════════════════════════════════════════════
#  Agent
# ═══════════════════════════════════════════════

class Agent:
    def __init__(self, mother_url, psk):
        self.mother_url = mother_url
        self.psk = psk
        self.ws = None
        self.child_id = None
        self.stop_event = threading.Event()
        self.reconnect_delay = 1.0
        self.send_lock = threading.Lock()
        self.tunnel_streams = {}
        self.tunnel_lock = threading.RLock()

    def _build_url(self):
        url = self.mother_url
        if self.psk:
            sep = "&" if "?" in url else "?"
            url += f"{sep}key={self.psk}"
        return url

    def send_msg(self, msg):
        with self.send_lock:
            if self.ws is None:
                return False
            try:
                data = _msgpack_pack(msg)
                self.ws.send(data, OP_BINARY)
                return True
            except Exception:
                return False

    def _recv_loop(self):
        while not self.stop_event.is_set():
            try:
                opcode, data = self.ws.recv(timeout=1)
                if opcode == OP_BINARY and data:
                    msg = msgpack_unpack(data)
                    self._dispatch(msg)
            except socket.timeout:
                continue
            except ConnectionError:
                break
            except Exception:
                if not self.stop_event.is_set():
                    continue

    def _dispatch(self, msg):
        msg_type = msg.get("type", "")
        payload = msg.get("payload", {})

        if msg_type == TYPE_REGISTERED:
            self.child_id = payload.get("child_id", "")
        elif msg_type == TYPE_HEARTBEAT:
            pass
        elif msg_type == TYPE_TASK:
            t = threading.Thread(target=self._execute_task, args=(payload,), daemon=True)
            t.start()
        elif msg_type == TYPE_TUNNEL_OPEN:
            t = threading.Thread(target=self._handle_tunnel, args=(payload,), daemon=True)
            t.start()
        elif msg_type == TYPE_TUNNEL_DATA:
            tid = payload.get("tunnel_id", "")
            data = payload.get("data", b"")
            if isinstance(data, str):
                data = data.encode('utf-8', errors='replace')
            with self.tunnel_lock:
                q = self.tunnel_streams.get(tid)
            if q:
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass

    def _execute_task(self, task):
        task_id = task.get("task_id", "")
        command = task.get("command", "")
        args = task.get("args", [])
        env = task.get("env", {})
        timeout = task.get("timeout", 0)

        start = time.time()
        exit_code = 0
        stdout = b""
        stderr = b""

        try:
            child_env = os.environ.copy()
            child_env.update(env)
            cmd = [command] + args if args else [command]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=child_env,
            )
            try:
                if timeout > 0:
                    stdout, stderr = proc.communicate(timeout=timeout)
                else:
                    stdout, stderr = proc.communicate()
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                exit_code = -1
            else:
                exit_code = proc.returncode
        except FileNotFoundError:
            exit_code = 127
            stderr = f"command not found: {command}".encode()
        except Exception as e:
            exit_code = -1
            stderr = str(e).encode()

        duration = int((time.time() - start) * 1000)
        result = make_msg(TYPE_TASK_RESULT, {
            "task_id": task_id,
            "exit_code": exit_code,
            "stdout": stdout.decode("utf-8", errors="replace"),
            "stderr": stderr.decode("utf-8", errors="replace"),
            "duration_ms": duration,
        })
        self.send_msg(result)

    def _handle_tunnel(self, payload):
        tunnel_id = payload.get("tunnel_id", "")
        target = payload.get("target", "")

        try:
            if ":" in target:
                host, port = target.split(":", 1)
                port = int(port)
            else:
                host, port = target, 80
            sock = socket.create_connection((host, port), timeout=10)
        except Exception:
            return

        # tunnel_ready
        self.send_msg(make_msg(TYPE_TUNNEL_READY, {"tunnel_id": tunnel_id}))

        q = queue.Queue(maxsize=64)
        with self.tunnel_lock:
            self.tunnel_streams[tunnel_id] = q

        stop_tunnel = threading.Event()

        def read_from_target():
            try:
                while not self.stop_event.is_set() and not stop_tunnel.is_set():
                    data = sock.recv(32768)
                    if not data:
                        break
                    self.send_msg(make_msg(TYPE_TUNNEL_DATA, {
                        "tunnel_id": tunnel_id,
                        "data": data,
                    }))
            except Exception:
                pass
            finally:
                stop_tunnel.set()

        t = threading.Thread(target=read_from_target, daemon=True)
        t.start()

        try:
            while not self.stop_event.is_set() and not stop_tunnel.is_set():
                try:
                    data = q.get(timeout=1)
                    sock.sendall(data)
                except queue.Empty:
                    continue
                except Exception:
                    break
        finally:
            stop_tunnel.set()
            with self.tunnel_lock:
                self.tunnel_streams.pop(tunnel_id, None)
            try:
                sock.close()
            except Exception:
                pass

    def _heartbeat_loop(self):
        seq = 0
        while not self.stop_event.is_set():
            interval = 8 + random.randint(0, 17)
            if self.stop_event.wait(timeout=interval):
                break
            self.send_msg(make_msg(TYPE_HEARTBEAT, {"seq": seq}))
            seq += 1

    def _monitor_loop(self):
        while not self.stop_event.is_set():
            if self.stop_event.wait(timeout=15):
                break
            report = collect_report()
            self.send_msg(make_msg(TYPE_REPORT, report))

    def _connect(self):
        self.ws = WebSocket()
        self.ws.connect(self._build_url())

        # Register
        hostname = socket.gethostname()
        self.send_msg(make_msg(TYPE_REGISTER, {
            "hostname": hostname,
            "os": sys.platform,
            "arch": "python",
            "version": "dev",
        }))

        # Background loops
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()

        self._recv_loop()

    def run(self):
        while not self.stop_event.is_set():
            try:
                self._connect()
                self.reconnect_delay = 1.0
            except Exception:
                if not self.stop_event.is_set():
                    delay = self.reconnect_delay
                    self.reconnect_delay = min(self.reconnect_delay * 2, 60)
                    self.stop_event.wait(timeout=delay)

    def stop(self):
        self.stop_event.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


def main():
    agent = Agent(MOTHER_URL, MOTHER_KEY)

    def on_signal(sig, frame):
        agent.stop()
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    agent.run()


if __name__ == "__main__":
    main()