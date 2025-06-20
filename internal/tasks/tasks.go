package tasks

import (
	"fmt"
	"time"

	gtasks "google.golang.org/api/tasks/v1"
)

type Task struct {
	ID           int `json:"id"`
	GoogleID     string
	TodoistID    string
	Title        string    `json:"title"`
	Description  string    `json:"description,omitempty"`
	Due          time.Time `json:"due"`
	Completed    bool      `json:"status"`
	Notified     bool      `json:"notified"`
	Deleted      bool      `json:"deleted"`
	LastModified time.Time `json:"last_modified"`
}

type Tasks []Task

func (tasks Tasks) FindByID(id int) (*Task, bool) {
	for _, t := range tasks {
		if t.ID == id {
			return &t, true
		}
	}
	return nil, false
}

func (tasks Tasks) FindTaskByGoogleID(googleId string) (*Task, bool) {
	for _, t := range tasks {
		if t.GoogleID == googleId {
			return &t, true
		}
	}
	return nil, false
}

func (t Task) GTask() *gtasks.Task {
	var g gtasks.Task
	g.Title = t.Title
	g.Notes = t.Description
	g.Id = t.GoogleID
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

func (t Task) Task() string {
	if t.Description != "" {
		return fmt.Sprintf("ID:%d\n\tGoogle ID:%s\n\tTitle: %s\n\tDescription:%s\n\tDue:%s\n\tCompleted:%t\n\tModified:%s\n\tDeleted:%t", t.ID, t.GoogleID, t.Title, t.Description, t.Due, t.Completed, t.LastModified, t.Deleted)
	}
	return fmt.Sprintf("ID:%d\n\tGoogle ID:%s\n\tTitle: %s\n\tDue:%s\n\tCompleted:%t\n\tModified:%s\n\tDeleted:%t", t.ID, t.GoogleID, t.Title, t.Due, t.Completed, t.LastModified, t.Deleted)
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

type TasksList struct {
	ID       string
	GoogleID string
	Title    string
}

type TasksLists []TasksList
