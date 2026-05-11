package mother

import (
	"log"
	"sync"
	"time"

	"github.com/huanxherta/hx-snack/internal/protocol"
)

// TaskStatus represents the lifecycle of a task.
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskTimeout   TaskStatus = "timeout"
)

// TaskRecord stores full task lifecycle.
type TaskRecord struct {
	ID        string     `json:"id"`
	ChildID   string     `json:"child_id"`
	Command   string     `json:"command"`
	Args      []string   `json:"args"`
	Status    TaskStatus `json:"status"`
	ExitCode  int        `json:"exit_code"`
	Stdout    string     `json:"stdout"`
	Stderr    string     `json:"stderr"`
	Duration  int64      `json:"duration_ms"`
	CreatedAt time.Time  `json:"created_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	Result    chan *protocol.TaskResultPayload `json:"-"`
}

// TaskQueue manages task dispatch and lifecycle.
type TaskQueue struct {
	mu    sync.RWMutex
	tasks map[string]*TaskRecord
	hub   *Hub

	// Callbacks
	onComplete func(*TaskRecord)
}

// NewTaskQueue creates a task queue.
func NewTaskQueue(hub *Hub) *TaskQueue {
	return &TaskQueue{
		tasks: make(map[string]*TaskRecord),
		hub:   hub,
	}
}

// Submit sends a task to a child and returns a task ID.
func (tq *TaskQueue) Submit(childID, command string, args []string, timeout int) (*TaskRecord, error) {
	taskID := generateID()

	// Pre-check child exists
	tq.hub.mu.RLock()
	_, ok := tq.hub.children[childID]
	tq.hub.mu.RUnlock()
	if !ok {
		return nil, ErrChildNotFound
	}

	record := &TaskRecord{
		ID:        taskID,
		ChildID:   childID,
		Command:   command,
		Args:      args,
		Status:    TaskPending,
		CreatedAt: time.Now(),
		Result:    make(chan *protocol.TaskResultPayload, 1),
	}

	tq.mu.Lock()
	tq.tasks[taskID] = record
	tq.mu.Unlock()

	// Send to child
	err := tq.hub.sendTask(childID, taskID, command, args, timeout)
	if err != nil {
		tq.mu.Lock()
		delete(tq.tasks, taskID)
		tq.mu.Unlock()
		return nil, err
	}

	now := time.Now()
	record.Status = TaskRunning
	record.StartedAt = &now

	log.Printf("[task] %s dispatched to %s: %s %v", taskID, childID, command, args)

	// If timeout set, start timer
	if timeout > 0 {
		go func() {
			timer := time.NewTimer(time.Duration(timeout) * time.Second)
			select {
			case <-timer.C:
				if record.Status == TaskRunning {
					tq.completeTask(taskID, &protocol.TaskResultPayload{
						TaskID:   taskID,
						ExitCode: -1,
						Stderr:   "task timed out",
						Duration: int64(timeout) * 1000,
					}, TaskTimeout)
				}
			case <-record.Result:
				timer.Stop()
			}
		}()
	}

	return record, nil
}

// CompleteTask marks a task as done (called from hub on task_result).
func (tq *TaskQueue) CompleteTask(taskID string, result *protocol.TaskResultPayload) {
	status := TaskCompleted
	if result.ExitCode != 0 {
		status = TaskFailed
	}
	tq.completeTask(taskID, result, status)
}

func (tq *TaskQueue) completeTask(taskID string, result *protocol.TaskResultPayload, status TaskStatus) {
	tq.mu.Lock()
	record, ok := tq.tasks[taskID]
	if !ok {
		tq.mu.Unlock()
		return
	}
	record.Status = status
	record.ExitCode = result.ExitCode
	record.Stdout = result.Stdout
	record.Stderr = result.Stderr
	record.Duration = result.Duration
	now := time.Now()
	record.EndedAt = &now
	tq.mu.Unlock()

	// Signal completion
	select {
	case record.Result <- result:
	default:
	}

	if tq.onComplete != nil {
		tq.onComplete(record)
	}

	log.Printf("[task] %s %s: exit=%d duration=%dms", taskID, status, result.ExitCode, result.Duration)
}

// Wait blocks until the task completes or times out.
func (tq *TaskQueue) Wait(taskID string, timeout time.Duration) (*TaskRecord, error) {
	tq.mu.RLock()
	record, ok := tq.tasks[taskID]
	tq.mu.RUnlock()
	if !ok {
		return nil, &childNotFoundError{}
	}

	select {
	case <-record.Result:
		tq.mu.RLock()
		r := tq.tasks[taskID]
		tq.mu.RUnlock()
		return r, nil
	case <-time.After(timeout):
		return record, nil
	}
}

// GetTask returns a task by ID.
func (tq *TaskQueue) GetTask(taskID string) *TaskRecord {
	tq.mu.RLock()
	defer tq.mu.RUnlock()
	return tq.tasks[taskID]
}

// ListTasks returns all tasks, optionally filtered by child.
func (tq *TaskQueue) ListTasks(childID string) []*TaskRecord {
	tq.mu.RLock()
	defer tq.mu.RUnlock()

	var list []*TaskRecord
	for _, t := range tq.tasks {
		if childID == "" || t.ChildID == childID {
			list = append(list, t)
		}
	}
	return list
}

// OnComplete registers a completion callback.
func (tq *TaskQueue) OnComplete(fn func(*TaskRecord)) {
	tq.onComplete = fn
}

// Cleanup removes old completed tasks.
func (tq *TaskQueue) Cleanup(maxAge time.Duration) int {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	count := 0
	for id, t := range tq.tasks {
		if t.EndedAt != nil && time.Since(*t.EndedAt) > maxAge {
			delete(tq.tasks, id)
			count++
		}
	}
	return count
}