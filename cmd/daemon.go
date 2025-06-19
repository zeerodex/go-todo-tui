package cmd

import (
	"github.com/spf13/cobra"

	"github.com/zeerodex/goot/internal/daemon"
	"github.com/zeerodex/goot/internal/services"
)

func NewDaemonCmd(s services.TaskService) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Start a daemon of gootodo",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			daemon.StartDaemon(s)
		},
	}
	return cmd
}
