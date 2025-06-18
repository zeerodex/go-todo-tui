package components

import (
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/zeerodex/goot/internal/tasks"
	"github.com/zeerodex/goot/internal/workers"
)

var (
	mainColor  = "12"
	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color(mainColor)).
			Padding(0, 1)

	selectedTitleStyle = lipgloss.NewStyle().
				Border(lipgloss.NormalBorder(), false, false, false, true).
				BorderForeground(lipgloss.Color(mainColor)).
				Foreground(lipgloss.Color(mainColor)).
				Padding(0, 1)

	selectedDescStyle = selectedTitleStyle.
				Foreground(lipgloss.Color(mainColor))

	statusMessageStyle = lipgloss.NewStyle().
				Foreground(lipgloss.AdaptiveColor{Light: "14", Dark: mainColor})
	statusMessageErrStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("9"))
)

type ListErrorMsg struct {
	Err error
}

type item struct {
	id          int
	title, desc string
	completed   bool
}

func (i item) ID() int             { return i.id }
func (i item) Title() string       { return i.title }
func (i item) TitleOnly() string   { return strings.Fields(i.title)[0] }
func (i item) Description() string { return i.desc }
func (i item) Completed() bool     { return i.completed }
func (i item) FilterValue() string { return i.title }

type listKeyMap struct {
	deleteTask     key.Binding
	createTask     key.Binding
	updateTask     key.Binding
	toogleComplete key.Binding
	syncTasks      key.Binding
}

func newListKeyMap() *listKeyMap {
	return &listKeyMap{
		createTask: key.NewBinding(
			key.WithKeys("c"),
			key.WithHelp("c", "create"),
		),
		deleteTask: key.NewBinding(
			key.WithKeys("d"),
			key.WithHelp("d", "delete"),
		),
		updateTask: key.NewBinding(
			key.WithKeys("u"),
			key.WithHelp("u", "update"),
		),
		toogleComplete: key.NewBinding(
			key.WithKeys("t"),
			key.WithHelp("t", "toggle completed"),
		),
		syncTasks: key.NewBinding(
			key.WithKeys("s"),
			key.WithHelp("s", "sync tasks"),
		),
	}
}

type ListModel struct {
	list      list.Model
	keys      *listKeyMap
	IsLoading bool

	Method   string
	Selected item
}

func (m *ListModel) SetTasksCmd(tasks tasks.Tasks) tea.Cmd {
	items := make([]list.Item, len(tasks))
	for i, task := range tasks {
		items[i] = item{id: task.ID, title: task.FullTitle(), desc: task.Description, completed: task.Completed}
	}
	return func() tea.Msg { return setTasksMsg{items} }
}

func (m *ListModel) APIJobResultCmd(res *workers.APIJobResult) tea.Cmd {
	return func() tea.Msg {
		return apiJobResultMsg{res}
	}
}

type setTasksMsg struct {
	items []list.Item
}

type apiJobResultMsg struct {
	res *workers.APIJobResult
}

func (m ListModel) Init() tea.Cmd {
	return nil
}

func (m ListModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	if m.IsLoading {
		cmds = append(cmds, m.list.StartSpinner())
	} else {
		m.list.StopSpinner()
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		h, v := msg.Width, msg.Height
		m.list.SetSize(h, v)
	case tea.KeyMsg:
		if m.list.FilterState() != list.Filtering {
			switch {
			case key.Matches(msg, m.keys.deleteTask):
				if m.list.SelectedItem() != nil {
					m.Method = "delete"
					selected := m.list.SelectedItem().(item)
					statusCmd := m.list.NewStatusMessage(statusMessageStyle.Render("Deleted " + selected.TitleOnly()))
					m.Selected = selected
					m.IsLoading = true
					return m, statusCmd
				}
			case key.Matches(msg, m.keys.updateTask):
				if m.list.SelectedItem() != nil {
					m.Method = "update"
					selected := m.list.SelectedItem().(item)
					m.Selected = selected
					m.IsLoading = true
					return m, nil
				}
			case key.Matches(msg, m.keys.createTask):
				m.Method = "create"
				m.IsLoading = true
				return m, nil
			case key.Matches(msg, m.keys.toogleComplete):
				if m.list.SelectedItem() != nil {
					m.Method = "toogle"
					selected := m.list.SelectedItem().(item)
					statusCmd := m.list.NewStatusMessage(statusMessageStyle.Render("Toggle completed for " + selected.TitleOnly()))
					m.Selected = selected
					m.IsLoading = true
					return m, statusCmd
				}
			case key.Matches(msg, m.keys.syncTasks):
				m.Method = "sync"
				m.IsLoading = true
				return m, nil
			}
		}

	case apiJobResultMsg:
		m.IsLoading = false
		if msg.res.Err != nil {
			return m, m.list.NewStatusMessage(statusMessageErrStyle.Render("Error syncing tasks: " + msg.res.Err.Error()))
		}
		if msg.res.Operation == workers.SyncTasksOp {
			return m, m.list.NewStatusMessage(statusMessageStyle.Render("Tasks synced successfully!"))
		}
		return m, nil

	case setTasksMsg:
		return m, m.list.SetItems(msg.items)
	}

	newListModel, cmd := m.list.Update(msg)
	m.list = newListModel
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m ListModel) View() string {
	return m.list.View()
}

func InitialListModel() ListModel {
	var m ListModel

	var items []list.Item

	listKeys := newListKeyMap()
	delegate := list.NewDefaultDelegate()
	delegate.Styles.SelectedTitle = selectedTitleStyle
	delegate.Styles.SelectedDesc = selectedDescStyle

	list := list.New(items, delegate, 75, 30)
	list.Title = "Goot"
	list.Styles.Title = titleStyle

	list.StatusMessageLifetime = 4 * time.Second

	list.AdditionalShortHelpKeys = func() []key.Binding {
		return []key.Binding{
			listKeys.createTask,
			listKeys.updateTask,
			listKeys.deleteTask,
			listKeys.toogleComplete,
			listKeys.syncTasks,
		}
	}
	list.AdditionalFullHelpKeys = func() []key.Binding {
		return []key.Binding{
			listKeys.createTask,
			listKeys.updateTask,
			listKeys.deleteTask,
			listKeys.toogleComplete,
			listKeys.syncTasks,
		}
	}

	m = ListModel{list: list, keys: listKeys}

	return m
}
