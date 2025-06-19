package workers

import (
	"fmt"

	"github.com/zeerodex/goot/internal/models"
	"github.com/zeerodex/goot/internal/tasks"
)

func (w *Worker) Sync() error {
	ltasks, err := w.repo.GetAllTasks()
	if err != nil {
		return fmt.Errorf("failed to get all local tasks: %w", err)
	}
	deletedLTasks, err := w.repo.GetAllDeletedTasks()
	if err != nil {
		return fmt.Errorf("failed to get all deleted tasks: %w", err)
	}
	ltasks = append(ltasks, deletedLTasks...)

	apisTasks := make(map[string]tasks.Tasks)
	snapshots := make(models.Snapshots)
	for apiName, api := range w.apis {
		apisTasks[apiName], err = api.GetAllTasks()
		if err != nil {
			return fmt.Errorf("failed to get all tasks from %s api: %w", apiName, err)
		}
		snapshots[apiName], err = w.snapRepo.GetLastSnapshot(apiName)
		if err != nil {
			return fmt.Errorf("failed to get snapshot for %s api: %w", apiName, err)
		}
	}

	fmt.Println("SNAPSHOTS:")
	for apiName, snapshot := range snapshots {
		fmt.Print("\t")
		fmt.Print(apiName + ":\n")
		fmt.Print("\t\t")
		fmt.Print(snapshot.IDs)
		fmt.Print("\n")
	}
	fmt.Print("\n")
	fmt.Println("APIs:")
	for apiName, tasks := range apisTasks {
		fmt.Print("\t")
		fmt.Print(apiName + ":\n")
		fmt.Print("\t\t")
		fmt.Print(tasks)
		fmt.Print("\n")
	}

	if !apisTasksEqual(apisTasks) {
		deleted, added := getDiff(apisTasks, snapshots)
		fmt.Println("DELETED AND ADDED:")
		fmt.Println("DELETED")
		fmt.Println(deleted)
		fmt.Println("ADDED")
		fmt.Println(added)

		for sourceAPI, addedIds := range added {
			for _, addedId := range addedIds {
				task, _ := apisTasks[sourceAPI].FindTaskByAPIID(addedId, sourceAPI)
				for targetAPI, api := range w.apis {
					if targetAPI != sourceAPI {
						apiTask, err := api.CreateTask(task)
						if err != nil {
							return fmt.Errorf("failed to create task in %s api: %w", targetAPI, err)
						}
						task.APIIDs[targetAPI] = apiTask.APIIDs[targetAPI]
					}
				}
				if _, err := w.repo.CreateTask(task); err != nil {
					return fmt.Errorf("failed to create local task: %w", err)
				}
			}
		}
		for sourceAPI, deletedIds := range deleted {
			for _, deletedId := range deletedIds {
				task, found := ltasks.FindTaskByAPIID(deletedId, sourceAPI)
				fmt.Println(found)
				if found {
					for targetAPI := range w.apis {
						if targetAPI != sourceAPI {
							if err := w.deleteTaskFromAPI(task.ID, targetAPI); err != nil {
								return fmt.Errorf("failed to create task in %s api: %w", targetAPI, err)
							}
						}
					}
					if err := w.repo.DeleteTaskByID(task.ID); err != nil {
						return fmt.Errorf("failed to delete local task deleted in api: %w", err)
					}
				}
			}
		}
	}

	if err = w.processCreateSnapshotsOp(); err != nil {
		return fmt.Errorf("failed to create snapshots: %w", err)
	}

	for apiName, api := range w.apis {
		apisTasks[apiName], err = api.GetAllTasks()
		if err != nil {
			return fmt.Errorf("failed to get all tasks from %s api: %w", apiName, err)
		}
	}

	apiTasks := mergeAPIsTasks(apisTasks)
	if slicesEqual(ltasks, apiTasks) {
		return nil
	} else {
		if err = w.syncLTasks(ltasks, apiTasks); err != nil {
			return fmt.Errorf("failed to replace local tasks with api tasks: %w", err)
		}
	}

	return nil
}

func (w *Worker) syncLTasks(ltasks tasks.Tasks, apiTasks tasks.Tasks) error {
	var err error
	for _, apiTask := range apiTasks {
		task, found := ltasks.FindTaskByAPIID(apiTask.APIIDs[apiTask.Source], apiTask.Source)
		if found {
			if task.Deleted {
				for apiName, api := range w.apis {
					err = api.DeleteTaskByID(apiTask.APIIDs[apiName])
					if err != nil {
						return fmt.Errorf("local task was deleted: failed to delete task from %s api: %w", apiName, err)
					}
				}
				err = w.repo.DeleteTaskByID(task.ID)
				if err != nil {
					return fmt.Errorf("failed to delete task: %w", err)
				}
				continue
			}
			if !task.Equal(apiTask) {
				switch apiTask.LastModified.Compare(task.LastModified) {
				case 1:
					apiTask.ID = task.ID
					if _, err := w.repo.UpdateTask(&apiTask); err != nil {
						return fmt.Errorf("failed to update task api id: %w", err)
					}
					for apiName, api := range w.apis {
						if apiName != apiTask.Source {
							if _, err := api.UpdateTask(&apiTask); err != nil {
								return fmt.Errorf("failed to update task: %w", err)
							}
						}
					}
				case -1:
					for _, api := range w.apis {
						if _, err := api.UpdateTask(task); err != nil {
							return fmt.Errorf("failed to update task: %w", err)
						}
					}
				}
			}
		} else {
			_, err = w.repo.CreateTask(&apiTask)
			if err != nil {
				return fmt.Errorf("local task was not found: failed to create local task: %w", err)
			}
			continue
		}
	}
	return nil
}

func getDiff(apisTasks map[string]tasks.Tasks, snapshots models.Snapshots) (map[string][]string, map[string][]string) {
	apiTasksIds := make(map[string][]string, len(apisTasks))
	for apiName, tasks := range apisTasks {
		tasksIdsList := make([]string, len(tasks))
		for i, t := range tasks {
			tasksIdsList[i] = t.APIIDs[apiName]
		}
		apiTasksIds[apiName] = tasksIdsList
	}

	deleted := make(map[string][]string)
	added := make(map[string][]string)

	for apiName, snapshot := range snapshots {

		snapIds := make(map[string]bool)
		for _, id := range snapshot.IDs {
			snapIds[id] = true
		}
		apiIds := make(map[string]bool)
		for _, id := range apiTasksIds[apiName] {
			apiIds[id] = true
		}

		for snapId := range snapIds {
			if !apiIds[snapId] {
				deleted[apiName] = append(deleted[apiName], snapId)
			}
		}
		for apiId := range apiIds {
			if !snapIds[apiId] {
				added[apiName] = append(added[apiName], apiId)
			}
		}

	}

	return deleted, added
}

func apisTasksEqual(apisTasks map[string]tasks.Tasks) bool {
	if len(apisTasks) == 0 {
		return true
	}

	var reference tasks.Tasks
	for _, slice := range apisTasks {
		reference = slice
		break
	}

	for _, slice := range apisTasks {
		if !slicesEqual(reference, slice) {
			return false
		}
	}
	return true
}

func slicesEqual(slice1, slice2 tasks.Tasks) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	for i := range slice1 {
		if !slice1[i].Equal(slice2[i]) || slice1[i].Deleted != slice2[i].Deleted {
			return false
		}
	}
	return true
}

// All tasks slices must be equal
func mergeAPIsTasks(apisTasks map[string]tasks.Tasks) tasks.Tasks {
	var templateTasks tasks.Tasks
	for _, slice := range apisTasks {
		if len(slice) != 0 {
			templateTasks = slice
			break
		}
	}

	mergedTasks := make(tasks.Tasks, len(templateTasks))

	for i, task := range templateTasks {
		for apiName, apiTasks := range apisTasks {
			if len(apiTasks) != 0 {
				task.APIIDs[apiName] = apiTasks[i].APIIDs[apiName]
			}
		}
		mergedTasks[i] = task
	}
	return mergedTasks
}
