package tui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/zeerodex/goot/internal/services"
	"github.com/zeerodex/goot/internal/tasks"
	"github.com/zeerodex/goot/internal/tui/components"
	"github.com/zeerodex/goot/internal/workers"
)

type AppState int

const (
	ListView AppState = iota
	CreationView
	UpdateView
	ErrView
)

type MainModel struct {
	currentState  AppState
	previuosState AppState

	listModel     components.ListModel
	creationModel components.CreationModel

	tasks tasks.Tasks
	s     services.TaskService
	err   error
}

func syncTasksCmd(s services.TaskService) tea.Cmd {
	return func() tea.Msg {
		err := s.Sync()
		if err != nil {
			return errMsg{err: err}
		}
		return fetchTasksCmd(s)()
	}
}

func updateTaskCmd(s services.TaskService, task *tasks.Task) tea.Cmd {
	return func() tea.Msg {
		_, err := s.UpdateTask(task)
		if err != nil {
			return errMsg{err: err}
		}
		return fetchTasksCmd(s)()
	}
}

func fetchTasksCmd(s services.TaskService) tea.Cmd {
	return func() tea.Msg {
		tasks, err := s.GetAllTasks()
		if err != nil {
			return errMsg{err: err}
		}
		return fetchedTasksMsg{Tasks: tasks}
	}
}

func createTaskCmd(s services.TaskService, task *tasks.Task) tea.Cmd {
	return func() tea.Msg {
		_, err := s.CreateTask(task)
		if err != nil {
			return errMsg{err: err}
		}
		return fetchTasksCmd(s)()
	}
}

func deleteTaskCmd(s services.TaskService, id int) tea.Cmd {
	return func() tea.Msg {
		err := s.DeleteTaskByID(id)
		if err != nil {
			return errMsg{err: err}
		}
		return fetchTasksCmd(s)()
	}
}

func toggleCompletedCmd(s services.TaskService, id int, completed bool) tea.Cmd {
	return func() tea.Msg {
		err := s.ToggleCompleted(id, completed)
		if err != nil {
			return errMsg{err: err}
		}
		return fetchTasksCmd(s)()
	}
}

type syncTasksMsg struct{}

type fetchTasksMsg struct{}

type fetchedTasksMsg struct {
	Tasks tasks.Tasks
}

type deleteTaskMsg struct {
	id int
}

type toggleCompletedMsg struct {
	id        int
	completed bool
}

type createTaskMsg struct {
	Task *tasks.Task
}

type updateTaskMsg struct {
	id int
}

type updatedTaskMsg struct {
	Task *tasks.Task
}

type errMsg struct {
	err error
}

type APIErrMsg struct {
	Operation string
	TaskID    int
	Err       error
}

func (m MainModel) Init() tea.Cmd {
	return tea.Batch(fetchTasksCmd(m.s), listenForWorkerResults(m.s.WP().Result()))
}

func (m MainModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "esc":
			switch m.currentState {
			case CreationView:
				m.currentState = m.previuosState
			case ErrView:
				m.currentState = m.previuosState
			}
			return m, nil
		case "q":
			switch m.currentState {
			case ListView:
				return m, tea.Quit
			case ErrView:
				m.currentState = m.previuosState
			}
			return m, nil
		}
	case syncTasksMsg:
		cmds = append(cmds, syncTasksCmd(m.s))

	case deleteTaskMsg:
		cmds = append(cmds, deleteTaskCmd(m.s, msg.id))

	case toggleCompletedMsg:
		cmds = append(cmds, toggleCompletedCmd(m.s, msg.id, msg.completed))

	case fetchTasksMsg:
		cmds = append(cmds, fetchTasksCmd(m.s))

	case fetchedTasksMsg:
		m.tasks = msg.Tasks
		cmds = append(cmds, m.listModel.SetTasks(m.tasks))

	case updateTaskMsg:
		task, err := m.s.GetTaskByID(msg.id)
		if err != nil {
			m.err = err
			m.currentState = ErrView
		}
		m.creationModel = components.InitialUpdateModel(task)
		m.currentState = CreationView

	case updatedTaskMsg:
		cmds = append(cmds, updateTaskCmd(m.s, msg.Task))
		cmds = append(cmds, listenForWorkerResults(m.s.WP().Result()))
		m.creationModel = components.InitialCreationModel()

		m.currentState = m.previuosState

	case createTaskMsg:
		cmds = append(cmds, createTaskCmd(m.s, msg.Task), listenForWorkerResults(m.s.WP().Result()))
		m.creationModel = components.InitialCreationModel()

		m.currentState = m.previuosState

	case errMsg:
		m.err = fmt.Errorf("Error: %w", msg.err)
		m.previuosState = m.currentState
		m.currentState = ErrView

	case APIErrMsg:
		m.err = fmt.Errorf("API Error: failed to process %s operation on task ID %d: %w", msg.Operation, msg.TaskID, msg.Err)
		m.previuosState = m.currentState
		m.currentState = ErrView
	}

	switch m.currentState {
	case ListView:
		listModel, listCmd := m.listModel.Update(msg)
		m.listModel = listModel.(components.ListModel)
		cmds = append(cmds, listCmd)

		switch m.listModel.Method {
		case "delete":
			m.listModel.Method = ""
			cmds = append(cmds, func() tea.Msg {
				return deleteTaskMsg{id: m.listModel.Selected.ID()}
			})
		case "create":
			m.listModel.Method = ""
			m.previuosState = m.currentState
			m.currentState = CreationView
		case "update":
			m.listModel.Method = ""
			m.previuosState = m.currentState
			cmds = append(cmds, func() tea.Msg {
				return updateTaskMsg{id: m.listModel.Selected.ID()}
			})
		case "toogle":
			m.listModel.Method = ""
			cmds = append(cmds, func() tea.Msg {
				return toggleCompletedMsg{id: m.listModel.Selected.ID(), completed: !m.listModel.Selected.Completed()}
			})
		case "sync":
			m.listModel.Method = ""
			cmds = append(cmds, func() tea.Msg {
				return syncTasksMsg{}
			})
		}

	case CreationView:
		creationModel, creationCmd := m.creationModel.Update(msg)
		m.creationModel = creationModel.(components.CreationModel)
		cmds = append(cmds, creationCmd)

		if m.creationModel.Done {
			switch m.creationModel.Method {
			case "create":
				cmds = append(cmds, func() tea.Msg {
					return createTaskMsg{Task: m.creationModel.Task}
				})
			case "update":
				cmds = append(cmds, func() tea.Msg {
					return updatedTaskMsg{Task: m.creationModel.Task}
				})
			}
		}
	}

	return m, tea.Batch(cmds...)
}

func (m MainModel) View() string {
	switch m.currentState {
	case ListView:
		return m.listModel.View()
	case CreationView:
		return m.creationModel.View()
	case ErrView:
		return m.err.Error()
	}
	return ""
}

func InitialMainModel(s services.TaskService) MainModel {
	listModel := components.InitialListModel()
	creationModel := components.InitialCreationModel()

	m := MainModel{
		currentState:  ListView,
		listModel:     listModel,
		creationModel: creationModel,

		s: s,
	}

	return m
}

func listenForWorkerResults(results <-chan workers.APIJobResult) tea.Cmd {
	return func() tea.Msg {
		for res := range results {
			if !res.Success && res.Err != nil {
				return APIErrMsg{Err: res.Err, Operation: string(res.Operation), TaskID: res.TaskID}
			} else if res.Success && res.Operation == workers.SyncTasksOp {
				return fetchTasksMsg{}
			}
		}
		return nil
	}
}
