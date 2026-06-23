package child

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

type httpPollResponse struct {
	Messages []protocol.Message `json:"messages"`
}

func (a *Agent) connectHTTP(ctx context.Context) error {
	base := httpBaseFromMotherURL(a.MotherURL)
	if a.SSHTunnel && a.TunnelPort != "" {
		base = replaceHTTPHostPort(base, "localhost:"+a.TunnelPort)
	}
	a.httpBase = strings.TrimRight(base, "/")
	a.httpMode = true

	hostname, _ := os.Hostname()
	reg := map[string]string{
		"hostname": hostname,
		"os":       runtime.GOOS,
		"arch":     runtime.GOARCH,
		"version":  a.Version,
	}
	var resp struct {
		ChildID    string `json:"child_id"`
		HeartbeatS int    `json:"heartbeat_s"`
		Error      string `json:"error"`
	}
	if err := a.httpJSON(ctx, "POST", "/api/http/register", reg, &resp); err != nil {
		return err
	}
	if resp.ChildID == "" {
		if resp.Error != "" {
			return fmt.Errorf(resp.Error)
		}
		return fmt.Errorf("http register failed")
	}
	a.childID = resp.ChildID

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go a.httpMonitorLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		var poll httpPollResponse
		path := "/api/http/poll?id=" + url.QueryEscape(a.childID) + "&timeout=25"
		if err := a.httpJSON(ctx, "GET", path, nil, &poll); err != nil {
			return err
		}
		for i := range poll.Messages {
			a.handleMessage(&poll.Messages[i])
		}
	}
}

func (a *Agent) httpMonitorLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report := a.monitor.Collect()
			msg := protocol.NewMessage(protocol.TypeReport, report)
			a.httpPostMessages(ctx, []protocol.Message{msg})
		}
	}
}

func (a *Agent) httpPostMessages(ctx context.Context, msgs []protocol.Message) error {
	if a.childID == "" {
		return fmt.Errorf("missing child id")
	}
	body := map[string]interface{}{"messages": msgs}
	var out map[string]interface{}
	return a.httpJSON(ctx, "POST", "/api/http/messages?id="+url.QueryEscape(a.childID), body, &out)
}

func (a *Agent) httpJSON(ctx context.Context, method, path string, in interface{}, out interface{}) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.httpBase+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.PSK != "" {
		req.Header.Set("X-Child-Key", a.PSK)
	}
	res, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		return fmt.Errorf("http %d: %s", res.StatusCode, string(b))
	}
	if out != nil && len(b) > 0 {
		if err := json.Unmarshal(b, out); err != nil {
			return err
		}
	}
	return nil
}

func httpBaseFromMotherURL(raw string) string {
	if strings.HasPrefix(raw, "ws://") {
		raw = "http://" + strings.TrimPrefix(raw, "ws://")
	} else if strings.HasPrefix(raw, "wss://") {
		raw = "https://" + strings.TrimPrefix(raw, "wss://")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return strings.TrimRight(raw, "/")
	}
	return u.Scheme + "://" + u.Host
}

func replaceHTTPHostPort(raw, newHostPort string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return raw
	}
	u.Host = newHostPort
	u.Path = ""
	u.RawQuery = ""
	return strings.TrimRight(u.String(), "/")
}
