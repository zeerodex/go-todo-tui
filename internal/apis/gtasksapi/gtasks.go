package gtasksapi

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/api/option"
	gtasks "google.golang.org/api/tasks/v1"

	"github.com/zeerodex/goot/internal/apis"
	"github.com/zeerodex/goot/internal/tasks"
)

var (
	tokFile  = "gtasks_token.json"
	authURL  = "https://accounts.google.com/o/oauth2/auth"
	tokenURL = "https://oauth2.googleapis.com/token"
)

func GetService() (*gtasks.Service, error) {
	clientID, clientSecret := os.Getenv("GOOGLE_CLIENT_ID"), os.Getenv("GOOGLE_CLIENT_SECRET")
	client, err := apis.NewOAuthHandler(
		clientID,
		clientSecret,
		authURL,
		tokenURL,
		tokFile,
		[]string{gtasks.TasksScope}).
		GetClient()
	if err != nil {
		// HACK:
		return nil, fmt.Errorf("failed to init oauth handler: %w", err)
	}
	srv, err := gtasks.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve tasks service: %w", err)
	}
	return srv, nil
}

type GTasksApi struct {
	srv *gtasks.Service

	ListId string
}

func NewGTasksApi(listId string) (apis.API, error) {
	srv, err := GetService()
	if err != nil {
		return nil, fmt.Errorf("failed to get gtasks service: %w", err)
	}
	return &GTasksApi{srv: srv, ListId: listId}, nil
}

func (api *GTasksApi) CreateTask(task *tasks.Task) (*tasks.Task, error) {
	gtask, err := api.srv.Tasks.Insert(api.ListId, GTask(task)).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to create task in list '%s': %w", api.ListId, err)
	}
	task.APIIDs[tasks.GTasks] = gtask.Id
	return task, nil
}

func (api *GTasksApi) GetTaskByID(id string) (*tasks.Task, error) {
	gtask, err := api.srv.Tasks.Get(api.ListId, id).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve task '%s' from list '%s': %w", id, api.ListId, err)
	}
	return Task(gtask), nil
}

func (api *GTasksApi) GetAllTasks() (tasks.Tasks, error) {
	gtasks, err := api.srv.Tasks.List(api.ListId).ShowCompleted(true).ShowHidden(true).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve all tasks from list '%s': %w", api.ListId, err)
	}

	tasksList := make(tasks.Tasks, len(gtasks.Items))
	for i, task := range gtasks.Items {
		t := Task(task)
		tasksList[i] = *t
	}
	return tasksList, nil
}

func (api *GTasksApi) DeleteTaskByID(id string) error {
	err := api.srv.Tasks.Delete(api.ListId, id).Do()
	if err != nil {
		return fmt.Errorf("failed to delete task '%s' from list '%s': %w", id, api.ListId, err)
	}
	return nil
}

func (api *GTasksApi) UpdateTask(task *tasks.Task) (*tasks.Task, error) {
	g, err := api.srv.Tasks.Patch(api.ListId, task.APIIDs[tasks.GTasks], GTask(task)).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to patch task '%s' from list '%s': %w", task.APIIDs[tasks.GTasks], api.ListId, err)
	}
	return Task(g), nil
}

func (api *GTasksApi) SetTaskCompleted(id string, completed bool) error {
	gtask := &gtasks.Task{}
	if completed {
		gtask.Status = "completed"
	} else {
		gtask.Status = "needsAction"
	}

	_, err := api.srv.Tasks.Patch(api.ListId, id, gtask).Do()
	if err != nil {
		return fmt.Errorf("failed to delete task '%s' from list '%s': %v", id, api.ListId, err)
	}
	return nil
}

func Task(g *gtasks.Task) *tasks.Task {
	t := &tasks.Task{
		Source:      tasks.GTasks,
		Title:       g.Title,
		Description: g.Notes,
		Completed:   g.Status == "completed",
		Deleted:     g.Deleted,
	}
	t.APIIDs = make(map[string]string)
	t.APIIDs[tasks.GTasks] = g.Id
	if g.Due != "" {
		t.Due, _ = time.Parse(time.RFC3339, g.Due)
	}
	t.LastModified, _ = time.Parse(time.RFC3339, g.Updated)
	if g.Completed != nil {
		completedTime, _ := time.Parse(time.RFC3339, *g.Completed)
		timeDiff := t.LastModified.Compare(completedTime)
		if timeDiff == -1 {
			t.LastModified = completedTime
		}
	}
	return t
}

func GTask(t *tasks.Task) *gtasks.Task {
	var g gtasks.Task
	g.Title = t.Title
	g.Notes = t.Description
	g.Id = t.APIIDs[tasks.GTasks]
	if t.Completed {
		g.Status = "completed"
	} else if !t.Completed {
		g.Status = "needsAction"
	}
	if !t.Due.IsZero() {
		g.Due = t.Due.Format(time.RFC3339)
	} else {
		g.Due = ""
	}
	return &g
}
