package mother

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

// ProxyHTTP forwards an HTTP request through a child node to the target host.
// target format: "host:port" (e.g. "api.openai.com:443")
func (h *Hub) ProxyHTTP(target string, w http.ResponseWriter, r *http.Request) {
	child := h.pickChild()
	if child == nil {
		http.Error(w, "no children online", 503)
		return
	}

	streamID := generateID()
	log.Printf("[proxy] %s -> %s via %s", streamID, target, child.ID)

	dataCh := make(chan []byte, 64)
	readyCh := h.registerProxyStream(streamID, dataCh)
	defer h.unregisterProxyStream(streamID)

	openMsg := protocol.NewMessage(protocol.TypeTunnelOpen, protocol.TunnelOpenPayload{
		TunnelID: streamID,
		Target:   target,
	})
	child.mu.Lock()
	b, _ := msgpack.Marshal(openMsg)
	child.Conn.WriteMessage(websocket.BinaryMessage, b)
	child.mu.Unlock()

	select {
	case <-readyCh:
	case <-time.After(10 * time.Second):
		http.Error(w, "tunnel timeout", 504)
		return
	}

	pw := &proxyWriter{child: child, streamID: streamID}
	pr := &proxyReader{ch: dataCh}

	// Determine scheme
	isTLS := strings.HasSuffix(target, ":443")
	outURL := *r.URL
	outURL.Host = target
	outURL.Host = strings.TrimSuffix(outURL.Host, ":80")
	outURL.Host = strings.TrimSuffix(outURL.Host, ":443")

	if isTLS {
		outURL.Scheme = "https"
	} else {
		outURL.Scheme = "http"
	}

	// Strip /p/host from path
	path := strings.TrimPrefix(r.URL.Path, "/p/")
	if idx := strings.Index(path, "/"); idx >= 0 {
		outURL.Path = path[idx:]
	} else {
		outURL.Path = "/"
	}
	if r.URL.RawQuery != "" {
		outURL.RawQuery = r.URL.RawQuery
	}

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, outURL.String(), r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	for k, vv := range r.Header {
		for _, v := range vv {
			outReq.Header.Add(k, v)
		}
	}

	// Build a net.Conn from the tunnel for the HTTP client
	hostname, _, _ := net.SplitHostPort(target)
	tconn := &tunnelConn{reader: pr, writer: pw, host: hostname}

	var conn net.Conn = tconn
	if isTLS {
		tlsConn := tls.Client(tconn, &tls.Config{ServerName: hostname})
		if err := tlsConn.Handshake(); err != nil {
			log.Printf("[proxy] %s TLS handshake error: %v", streamID, err)
			http.Error(w, "TLS handshake failed", 502)
			return
		}
		conn = tlsConn
	}

	if err := outReq.Write(conn); err != nil {
		log.Printf("[proxy] %s write error: %v", streamID, err)
		http.Error(w, "proxy write failed", 502)
		return
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), outReq)
	if err != nil {
		log.Printf("[proxy] %s read error: %v", streamID, err)
		http.Error(w, "proxy read failed", 502)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Hub) pickChild() *ChildState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.children {
		return c
	}
	return nil
}

func (h *Hub) registerProxyStream(streamID string, ch chan []byte) <-chan struct{} {
	h.tunnelMu.Lock()
	defer h.tunnelMu.Unlock()

	if h.tunnelStreams == nil {
		h.tunnelStreams = make(map[string]chan []byte)
	}
	if h.tunnelReady == nil {
		h.tunnelReady = make(map[string]chan struct{})
	}

	h.tunnelStreams[streamID] = ch
	ready := make(chan struct{})
	h.tunnelReady[streamID] = ready
	return ready
}

func (h *Hub) unregisterProxyStream(streamID string) {
	h.tunnelMu.Lock()
	delete(h.tunnelStreams, streamID)
	delete(h.tunnelReady, streamID)
	h.tunnelMu.Unlock()
}

// tunnelConn adapts proxyReader+proxyWriter into a net.Conn for TLS wrapping.
type tunnelConn struct {
	reader *proxyReader
	writer *proxyWriter
	host   string
}

func (tc *tunnelConn) Read(b []byte) (int, error)  { return tc.reader.Read(b) }
func (tc *tunnelConn) Write(b []byte) (int, error) { return tc.writer.Write(b) }
func (tc *tunnelConn) Close() error                 { return nil }
func (tc *tunnelConn) LocalAddr() net.Addr          { return fakeAddr("tunnel") }
func (tc *tunnelConn) RemoteAddr() net.Addr         { return fakeAddr(tc.host) }
func (tc *tunnelConn) SetDeadline(t time.Time) error      { return nil }
func (tc *tunnelConn) SetReadDeadline(t time.Time) error  { return nil }
func (tc *tunnelConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeAddr string

func (f fakeAddr) Network() string { return "tcp" }
func (f fakeAddr) String() string  { return string(f) }

// proxyWriter writes to the child's WS as tunnel_data.
type proxyWriter struct {
	child    *ChildState
	streamID string
}

func (pw *proxyWriter) Write(p []byte) (int, error) {
	msg := protocol.NewMessage(protocol.TypeTunnelData, protocol.TunnelDataPayload{
		TunnelID: pw.streamID,
		Data:     p,
	})
	pw.child.mu.Lock()
	b, _ := msgpack.Marshal(msg)
	err := pw.child.Conn.WriteMessage(websocket.BinaryMessage, b)
	pw.child.mu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("ws write: %w", err)
	}
	return len(p), nil
}

// proxyReader reads from the tunnel data channel.
type proxyReader struct {
	ch   chan []byte
	buf  []byte
	done bool
}

func (pr *proxyReader) Read(p []byte) (int, error) {
	if len(pr.buf) > 0 {
		n := copy(p, pr.buf)
		pr.buf = pr.buf[n:]
		return n, nil
	}
	if pr.done {
		return 0, io.EOF
	}
	data, ok := <-pr.ch
	if !ok {
		pr.done = true
		return 0, io.EOF
	}
	n := copy(p, data)
	if n < len(data) {
		pr.buf = data[n:]
	}
	return n, nil
}