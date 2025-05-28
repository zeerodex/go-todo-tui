package workers

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/zeerodex/goot/internal/apis"
	"github.com/zeerodex/goot/internal/repositories"
	"github.com/zeerodex/goot/internal/tasks"
)

type APIOperation string

const (
	CreateTaskOp      APIOperation = "create_task"
	UpdateTaskOp      APIOperation = "update_task"
	DeleteTaskOp      APIOperation = "delete_task"
	ToggleCompletedOp APIOperation = "toggle_completed_task"
	SyncTasksOp       APIOperation = "sync_tasks"
)

type APIJob struct {
	ID           int
	Operation    APIOperation
	Task         *tasks.Task
	TaskID       int
	TaskGoogleID string
	Retry        int
	Completed    bool
}

type APIJobResult struct {
	JobID        int
	Operation    APIOperation
	TaskID       int
	TaskGoogleID string
	Success      bool
	Err          error
}

type APIWorkerPool struct {
	numWorkers int
	workers    []*Worker
	jobQueue   chan APIJob
	resQueue   chan APIJobResult
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	started    bool
	mu         sync.RWMutex
}

func NewAPIWorkerPool(numWorkers int, queueSize int, apis []apis.API, repo repositories.TaskRepository) *APIWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	wp := &APIWorkerPool{
		numWorkers: numWorkers,
		workers:    make([]*Worker, numWorkers),
		jobQueue:   make(chan APIJob, queueSize),
		resQueue:   make(chan APIJobResult, queueSize),
		ctx:        ctx,
		cancel:     cancel,
		started:    false,
	}

	for i := range wp.numWorkers {
		wp.workers[i] = NewWorker(i, wp.jobQueue, wp.resQueue, apis, repo)
	}
	return wp
}

func (wp *APIWorkerPool) Start() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if wp.started {
		return
	}

	wp.started = true

	for _, w := range wp.workers {
		wp.wg.Add(1)
		go w.Start(wp.ctx, &wp.wg)
	}
}

func (wp *APIWorkerPool) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if !wp.started {
		return
	}

	wp.cancel()
	close(wp.jobQueue)
	wp.wg.Wait()
	close(wp.resQueue)
	wp.started = false
}

func (wp *APIWorkerPool) Submit(job APIJob) error {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if !wp.started {
		return errors.New("API worker pool not started")
	}

	select {
	case wp.jobQueue <- job:
		return nil
	case <-wp.ctx.Done():
		return errors.New("failed to submit job due to context cancellation")
	case <-time.After(5 * time.Second):
		return errors.New("job submission timeout")
	}
}

func (wp *APIWorkerPool) Result() <-chan APIJobResult {
	return wp.resQueue
}
