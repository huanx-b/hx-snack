package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/huanxherta/hx-snack/internal/mother"
)

func main() {
	var (
		port = flag.Int("port", 8080, "HTTP listen port")
		psk  = flag.String("key", "", "Pre-shared key for child auth")
	)
	flag.Parse()

	hub := mother.NewHub(*psk)
	tm := mother.NewTunnelManager(hub)

	mux := http.NewServeMux()
	mother.SetupRoutes(mux, hub, tm)

	// Wrap to intercept /p/http:// and /p/https:// before ServeMux cleans //
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uri := r.RequestURI
		if strings.HasPrefix(uri, "/p/http://") || strings.HasPrefix(uri, "/p/https://") {
			path := strings.TrimPrefix(uri, "/p/")
			useTLS := strings.HasPrefix(path, "https://")
			if useTLS {
				path = path[8:]
			} else {
				path = path[7:]
			}

			target, rest := path, "/"
			if idx := strings.Index(path, "/"); idx >= 0 {
				target = path[:idx]
				rest = path[idx:]
			}
			if qi := strings.Index(rest, "?"); qi >= 0 {
				rest = rest[:qi]
			}

			if !strings.Contains(target, ":") {
				if useTLS {
					target += ":443"
				} else {
					target += ":80"
				}
			}

			r.URL.Path = "/p/" + target + rest
			r.URL.RawQuery = ""
			r.URL.RawPath = ""
			r.RequestURI = r.URL.String()

			hub.ProxyHTTP(target, w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[mother] hxの偷吃 Mother listening on %s", addr)
	log.Printf("[mother] WebUI: http://localhost%s", addr)
	log.Printf("[mother] Children WS: ws://xxx%s/ws", addr)

	if err := http.ListenAndServe(addr, wrapped); err != nil {
		log.Fatalf("[mother] fatal: %v", err)
	}
}