package child

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/huanxherta/hx-snack/internal/protocol"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

// Monitor collects system metrics.
type Monitor struct {
	prevNetRx uint64
	prevNetTx uint64
	prevTime  time.Time
	bootTime  uint64
	hostname  string
}

// NewMonitor creates a new monitor.
func NewMonitor() *Monitor {
	hn, _ := os.Hostname()
	bt, _ := host.BootTime()
	return &Monitor{
		hostname: hn,
		bootTime: bt,
	}
}

// Collect gathers current system metrics.
func (m *Monitor) Collect() protocol.ReportPayload {
	r := protocol.ReportPayload{}

	// CPU
	cpuInfo, _ := cpu.Info()
	if len(cpuInfo) > 0 {
		cp, _ := cpu.Percent(0, false)
		if len(cp) > 0 {
			r.CPUPercent = cp[0]
		}
	} else {
		ld, _ := load.Avg()
		r.CPUPercent = ld.Load1
	}

	// Memory
	memInfo, _ := mem.VirtualMemory()
	if memInfo != nil {
		r.MemUsedBytes = memInfo.Used
		r.MemTotalBytes = memInfo.Total
	}

	// Disk — root partition
	diskInfo, _ := disk.Usage("/")
	if diskInfo != nil {
		r.DiskUsedBytes = diskInfo.Used
		r.DiskTotalBytes = diskInfo.Total
	}

	// Network — aggregate all interfaces
	netIO, _ := net.IOCounters(false)
	if len(netIO) > 0 {
		nowRx := netIO[0].BytesRecv
		nowTx := netIO[0].BytesSent
		r.NetRxBytes = nowRx
		r.NetTxBytes = nowTx

		// Store for next delta calculation
		m.prevNetRx = nowRx
		m.prevNetTx = nowTx
		m.prevTime = time.Now()
	}

	// Uptime
	bt, _ := host.BootTime()
	bt64 := bt
	if bt64 == 0 {
		bt64 = m.bootTime
	}
	r.UptimeSeconds = int64(time.Now().Unix()) - int64(bt64)

	return r
}

// Info returns system info for registration.
type SysInfo struct {
	Hostname string
	OS       string
	Arch     string
	NumCPU   int
	GoVer    string
}

func (m *Monitor) Info() SysInfo {
	return SysInfo{
		Hostname: m.hostname,
		OS:       runtime.GOOS,
		Arch:     runtime.GOARCH,
		NumCPU:   runtime.NumCPU(),
		GoVer:    runtime.Version(),
	}
}

// FormatBytes formats bytes to human-readable string.
func FormatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}