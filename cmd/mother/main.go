package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/huanxherta/hx-snack/internal/mother"
)

func main() {
	var (
		port   = flag.Int("port", 8080, "HTTP listen port")
		psk    = flag.String("key", "", "Pre-shared key for child auth")
	)
	flag.Parse()

	hub := mother.NewHub(*psk)
	tm := mother.NewTunnelManager(hub)

	mux := http.NewServeMux()
	mother.SetupRoutes(mux, hub, tm)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[mother] hxの偷吃 Mother listening on %s", addr)
	log.Printf("[mother] WebUI: http://localhost%s", addr)
	log.Printf("[mother] Children WS: ws://xxx%s/ws", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[mother] fatal: %v", err)
	}
}