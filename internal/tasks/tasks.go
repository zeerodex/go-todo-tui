package tasks

import (
	"fmt"
	"time"
)

const (
	Todoist = "todoist"
	GTasks  = "gtasks"
)

type Task struct {
	ID           int
	APIIDs       map[string]string
	Source       string
	Title        string
	Description  string
	Due          time.Time
	Completed    bool
	Notified     bool
	Deleted      bool
	LastModified time.Time
}

type (
	Tasks []Task
)

func (tasks Tasks) FindByID(id int) (*Task, bool) {
	for _, t := range tasks {
		if t.ID == id {
			return &t, true
		}
	}
	return nil, false
}

func (tasks Tasks) FindTaskByAPIID(apiId string, apiName string) (*Task, bool) {
	for _, t := range tasks {
		if t.APIIDs[apiName] == apiId {
			return &t, true
		}
	}
	return nil, false
}

func (t *Task) Update(tt *Task) *Task {
	t.Title = tt.Title
	t.Description = tt.Description
	t.Due = tt.Due
	t.Completed = tt.Completed
	return t
}

func (t Task) Task() string {
	if t.Description != "" {
		return fmt.Sprintf("ID:%d\n\tAPI IDs :%v\n\tTitle: %s\n\tDescription:%s\n\tDue:%s\n\tCompleted:%t\n\tModified:%s\n\tDeleted:%t", t.ID, t.APIIDs, t.Title, t.Description, t.Due, t.Completed, t.LastModified, t.Deleted)
	}
	return fmt.Sprintf("ID:%d\n\tAPI IDs:%v\n\tTitle: %s\n\tDue:%s\n\tCompleted:%t\n\tModified:%s\n\tDeleted:%t", t.ID, t.APIIDs, t.Title, t.Due, t.Completed, t.LastModified, t.Deleted)
}

func (t *Task) DueStr() string {
	if !t.Due.IsZero() {
		if t.Due.Hour() == 0 && t.Due.Minute() == 0 {
			return t.Due.Format("2006-01-02")
		}
		return t.Due.Format("2006-01-02 15:04")
	}
	return ""
}

func (t *Task) SetDueAndLastModified(dueStr string, lastModifiedStr string) error {
	due, err := time.Parse(time.RFC3339, dueStr)
	if err != nil {
		return err
	}
	t.Due = due
	lastModified, err := time.Parse(time.RFC3339, lastModifiedStr)
	if err != nil {
		return err
	}
	t.LastModified = lastModified
	return nil
}

func (t Task) FullTitle() string {
	var title string
	title += t.Title
	if !t.Due.IsZero() {
		title += " | " + t.DueStr()
	}
	if t.Completed {
		title += " | Completed"
	} else {
		title += " | Uncompleted"
	}
	return title
}

func (t Task) Equal(tt Task) bool {
	return t.Title == tt.Title && t.Description == tt.Description && t.Due.Equal(tt.Due) && t.Completed == tt.Completed
}
