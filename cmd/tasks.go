package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/zeerodex/goot/internal/services"
	"github.com/zeerodex/goot/internal/tasks"
	"github.com/zeerodex/goot/internal/tui"
	"github.com/zeerodex/goot/pkg/timeutil"
)

func NewAllTasksCmd(s services.TaskService) *cobra.Command {
	var jsonFormat bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all tasks",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			tasks, err := s.GetAllTasks()
			if err != nil {
				cmd.Println(err)
				return
			}
			if jsonFormat {
				b, err := json.MarshalIndent(&tasks, "", " ")
				if err != nil {
					cmd.Println(err)
				}
				os.Stdout.Write(b)
				cmd.Println()
			} else {
				for _, task := range tasks {
					cmd.Println(task.Task())
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&jsonFormat, "json", "j", false, "Output in json format")
	return cmd
}

func NewCreateCmd(s services.TaskService) *cobra.Command {
	var description string
	var dueTimeStr string
	cmd := &cobra.Command{
		Use:   "add [title] [date (Today if none)]",
		Short: "Creates a task",
		Args:  cobra.RangeArgs(1, 4),
		Run: func(cmd *cobra.Command, args []string) {
			var task tasks.Task
			task.Title = args[0]
			task.Description = description

			var dueStr string
			switch len(args) {
			case 4:
				dueStr = args[1] + " " + args[2] + " " + args[3]
			case 3:
				dueStr = args[1] + " " + args[2]
			case 2:
				dueStr = args[1]
			case 1:
				dueStr = "today"
			}

			if dueTimeStr != "" {
				dueStr += " " + dueTimeStr
			}

			due, err := timeutil.ParseAndValidateTimestamp(dueStr)
			if err != nil {
				cmd.Println(err.Error())
				return
			}
			task.Due = due

			_, err = s.CreateTask(&task)
			if err != nil {
				cmd.Printf("Error creating task: %v", err)
				return
			}
			cmd.Println(task.Task())
		},
	}
	cmd.Flags().StringVarP(&dueTimeStr, "time", "t", "", "Due time (HH:MM)")
	cmd.Flags().StringVarP(&description, "description", "d", "", "Description of the task")
	return cmd
}

func NewDeleteTaskCmd(s services.TaskService) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rm [task id]",
		Short: "Deletes a task",
		Args:  cobra.RangeArgs(0, 1),
		Run: func(cmd *cobra.Command, args []string) {
			var id int
			if len(args) == 0 {
				tasks, err := s.GetAllTasks()
				if err != nil {
					cmd.Println(err)
					return
				}
				var ok bool
				id, ok = tui.ChooseTask(tasks)
				if !ok {
					cmd.Println("No tasks specified")
					return
				}
			} else {
				var err error
				id, err = strconv.Atoi(args[0])
				if err != nil {
					cmd.Printf("Incorrect task id: %v", err)
					return
				}
			}
			err := s.DeleteTaskByID(id)
			if err != nil {
				cmd.Println(err)
				return
			}
		},
	}
	return cmd
}

func NewDoneTaskCmd(s services.TaskService) *cobra.Command {
	return &cobra.Command{
		Use:   "done [task id]",
		Short: "Marks task completed",
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var id int
			if len(args) == 0 {
				tasks, err := s.GetAllTasks()
				if err != nil {
					return err
				}
				var ok bool
				id, ok = tui.ChooseTask(tasks)
				if !ok {
					cmd.Println("No tasks specified")
					return nil
				}
			} else {
				var err error
				id, err = strconv.Atoi(args[0])
				if err != nil {
					return fmt.Errorf("incorrect task id: %w", err)
				}
			}
			err := s.SetTaskCompleted(id, true)
			if err != nil {
				return fmt.Errorf("failed to mark task completed: %w", err)
			}
			return nil
		},
	}
}

func NewGetAllTodoistTasks() *cobra.Command {
	return &cobra.Command{
		Use: "todoist_get",
		Run: func(cmd *cobra.Command, args []string) {
			// _ := todoist.NewTodoistAPI("d796823ac5129b8ac4bd5df51fa2e7568e222b28")

			// dateStr := time.Now().Format("2006-01-02")
			// date, _ := time.Parse("2006-01-02", dateStr)
			// task := &tasks.Task{
			// 	TodoistID:   "6c7Fh8rJ3X7FCJ24",
			// 	Title:       "title1",
			// 	Description: "escription",
			// 	Due:         time.Now(),
			// }
			// tasks, err := client.CreateTask(task)
			// tasks, err := client.PatchTask(task)
			// tasks, err := client.GetAllTasks()
			// err := client.SetTaskCompleted("6c7Fh8rJ3X7FCJ24", false)
			// if err != nil {
			// 	fmt.Println(err.Error())
			// 	return
			// }
			// fmt.Println(tasks)
		},
	}
}
