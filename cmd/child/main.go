package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/huanxherta/hx-snack/internal/child"
)

var Version = "dev"

func main() {
	var (
		motherURL = flag.String("mother", "", "Mother WebSocket URL (e.g., wss://host/ws)")
		psk       = flag.String("key", "", "Pre-shared key for auth")
	)
	flag.Parse()

	if *motherURL == "" {
		fmt.Println("Usage: child -mother wss://host/ws [-key xxx]")
		os.Exit(1)
	}

	agent := child.NewAgent(*motherURL, *psk, Version)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	log.Printf("[child] hxの偷吃 Child v%s starting...", Version)
	log.Printf("[child] connecting to mother: %s", *motherURL)

	if err := agent.Run(ctx); err != nil {
		log.Printf("[child] stopped: %v", err)
	}
}