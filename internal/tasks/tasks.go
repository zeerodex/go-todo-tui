package tasks

type Task struct {
	ID          int
	Title       string
	Description string
	Status      bool
}

type Tasks []Task
