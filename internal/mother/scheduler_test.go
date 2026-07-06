package mother

import (
	"testing"
	"time"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

func TestCompleteTaskPreservesTruncationFlags(t *testing.T) {
	tq := &TaskQueue{
		tasks: map[string]*TaskRecord{
			"task-1": {
				ID:        "task-1",
				Status:    TaskRunning,
				CreatedAt: time.Now(),
				Result:    make(chan *protocol.TaskResultPayload, 1),
			},
		},
	}

	tq.CompleteTask("task-1", &protocol.TaskResultPayload{
		TaskID:          "task-1",
		ExitCode:        0,
		Stdout:          "partial stdout",
		Stderr:          "partial stderr",
		StdoutTruncated: true,
		StderrTruncated: true,
		Duration:        123,
	})

	record := tq.GetTask("task-1")
	if record == nil {
		t.Fatal("task record missing")
	}
	if !record.StdoutTruncated {
		t.Fatal("stdout truncation flag was not preserved")
	}
	if !record.StderrTruncated {
		t.Fatal("stderr truncation flag was not preserved")
	}
}
