package workers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zeerodex/goot/internal/apis"
	"github.com/zeerodex/goot/internal/repositories"
	"github.com/zeerodex/goot/internal/tasks"
)

const (
	maxRetries = 3
	baseDelay  = time.Second
)

type Worker struct {
	ID       int
	jobQueue <-chan APIJob
	resultCh chan<- APIJobResult
	snapRepo repositories.SnapshotsRepository
	apis     map[string]apis.API
	repo     repositories.TaskRepository
}

func NewWorker(id int, jobChan <-chan APIJob, resChan chan<- APIJobResult, apis map[string]apis.API, repo repositories.TaskRepository) *Worker {
	return &Worker{
		ID:       id,
		jobQueue: jobChan,
		resultCh: resChan,
		snapRepo: repositories.NewAPISnapshotsRepository(repo.DB()),
		apis:     apis,
		repo:     repo,
	}
}

func (w *Worker) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-w.jobQueue:
			if !ok {
				return
			}
			result := w.processAPIJobWithRetry(ctx, job)

			select {
			case w.resultCh <- result:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) processAPIJobWithRetry(ctx context.Context, job APIJob) APIJobResult {
	var err error

	// for attempt := range maxRetries {
	// 	if attempt > 0 {
	// 		delay := time.Duration(attempt) * baseDelay
	// 		select {
	// 		case <-time.After(delay):
	// 		case <-ctx.Done():
	// 			break
	// 		}
	// 	}

	err = w.processAPIJob(job)
	// if err == nil {
	// 	break
	// }
	// }

	return APIJobResult{
		JobID:     job.ID,
		Operation: job.Operation,
		TaskID:    job.TaskID,
		Err:       err,
	}
}

func (w *Worker) processAPIJob(job APIJob) error {
	switch job.Operation {
	case SetTaskCompletedOp:
		return w.processSetTaskCompletedOp(job.TaskID, job.Completed)
	case UpdateTaskOp:
		return w.processUpdateTaskOp(job.Task)
	case DeleteTaskOp:
		return w.processDeleteTaskOp(job.TaskID)
	case CreateTaskOp:
		return w.processCreateTaskOp(job.Task)
	case SyncTasksOp:
		return w.processSyncTasksOp()
	case CreateSnapshotsOp:
		return w.processCreateSnapshotsOp()
	default:
		return fmt.Errorf("unknown operation: %v", job.Operation)
	}
}

func (w *Worker) processCreateTaskOp(task *tasks.Task) error {
	errChan := make(chan error, len(w.apis))
	var wg sync.WaitGroup

	wg.Add(len(w.apis))

	for apiName := range w.apis {
		go func(apiName string) {
			defer wg.Done()
			if err := w.createTaskInAPI(task, apiName); err != nil {
				errChan <- fmt.Errorf("failed to create task in %s: %w", apiName, err)
				return
			}
			errChan <- nil
		}(apiName)
	}

	wg.Wait()
	close(errChan)

	if err := w.processCreateSnapshotsOp(); err != nil {
		return fmt.Errorf("failed to create snapshot after creation: %w", err)
	}

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) createTaskInAPI(task *tasks.Task, apiName string) error {
	apiTask, err := w.apis[apiName].CreateTask(task)
	if err != nil {
		return fmt.Errorf("API creation failed: %w", err)
	}

	apiID, exists := apiTask.APIIDs[apiName]
	if !exists {
		return fmt.Errorf("API ID not found for %s", apiName)
	}

	return w.repo.UpdateTaskAPIID(task.ID, apiID, apiName)
}

func (w *Worker) processUpdateTaskOp(task *tasks.Task) error {
	errChan := make(chan error, len(w.apis))
	var wg sync.WaitGroup

	wg.Add(len(w.apis))

	for apiName, api := range w.apis {
		go func(apiName string, api apis.API) {
			defer wg.Done()
			if _, err := api.UpdateTask(task); err != nil {
				errChan <- fmt.Errorf("failed to update task in %s: %w", apiName, err)
				return
			}
			errChan <- nil
		}(apiName, api)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) processSetTaskCompletedOp(taskID int, completed bool) error {
	errChan := make(chan error, len(w.apis))
	var wg sync.WaitGroup

	wg.Add(len(w.apis))
	for apiName, api := range w.apis {
		go func(apiName string, api apis.API) {
			defer wg.Done()
			if err := w.setTaskCompletedInAPI(taskID, completed, apiName, api); err != nil {
				errChan <- fmt.Errorf("failed to set completion in %s: %w", apiName, err)
				return
			}
			errChan <- nil
		}(apiName, api)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) setTaskCompletedInAPI(taskID int, completed bool, apiName string, api apis.API) error {
	apiID, err := w.repo.GetTaskAPIID(taskID, apiName)
	if err != nil {
		return fmt.Errorf("failed to get API ID: %w", err)
	}

	return api.SetTaskCompleted(apiID, completed)
}

func (w *Worker) processDeleteTaskOp(taskID int) error {
	errChan := make(chan error, len(w.apis))
	var wg sync.WaitGroup

	wg.Add(len(w.apis))

	for apiName := range w.apis {
		go func(apiName string) {
			defer wg.Done()
			if err := w.deleteTaskFromAPI(taskID, apiName); err != nil {
				errChan <- fmt.Errorf("failed to delete task from %s: %w", apiName, err)
				return
			}
			errChan <- nil
		}(apiName)
	}

	wg.Wait()
	close(errChan)

	if err := w.processCreateSnapshotsOp(); err != nil {
		return fmt.Errorf("failed to create snapshot after deletion: %w", err)
	}

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) deleteTaskFromAPI(taskID int, apiName string) error {
	apiID, err := w.repo.GetTaskAPIID(taskID, apiName)
	if err != nil {
		return fmt.Errorf("failed to get API ID: %w", err)
	}

	return w.apis[apiName].DeleteTaskByID(apiID)
}

func (w *Worker) processSyncTasksOp() error {
	return w.Sync()
}

func (w *Worker) processCreateSnapshotsOp() error {
	errChan := make(chan error, len(w.apis))
	var wg sync.WaitGroup

	wg.Add(len(w.apis))
	for apiName, api := range w.apis {
		go func(apiName string, api apis.API) {
			defer wg.Done()
			if err := w.createSnapshotForAPI(apiName, api); err != nil {
				errChan <- fmt.Errorf("failed to create snapshot for %s: %w", apiName, err)
				return
			}
			errChan <- nil
		}(apiName, api)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) createSnapshotForAPI(apiName string, api apis.API) error {
	apiTasks, err := api.GetAllTasks()
	if err != nil {
		return fmt.Errorf("failed to get tasks: %w", err)
	}

	ids := make([]string, 0, len(apiTasks))
	for _, apiTask := range apiTasks {
		if apiID, exists := apiTask.APIIDs[apiName]; exists {
			ids = append(ids, apiID)
		}
	}

	err = w.snapRepo.CreateSnapshotForAPI(apiName, ids)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	return nil
}
