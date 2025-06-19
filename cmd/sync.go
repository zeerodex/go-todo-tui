package cmd

import (
	"github.com/spf13/cobra"

	"github.com/zeerodex/goot/internal/config"
	"github.com/zeerodex/goot/internal/services"
	"github.com/zeerodex/goot/internal/tui/components"
)

func NewSyncCmd(s services.TaskService, apis map[string]bool) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Enables sync with google tasks api",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := s.Sync()
			if err != nil {
				return err
			}
			res := <-s.WP().Results()
			if res.Err != nil {
				cmd.Println(res.Err.Error())
			}

			cmd.Println("Successfully synced APIs:")
			for api, enabled := range apis {
				if enabled {
					cmd.Println(" - " + api)
				}
			}
			return nil
		},
	}

	cmd.AddCommand(NewSyncOnStartupCmd())
	cmd.AddCommand(NewChooseSyncAPIs(apis))
	return cmd
}

func NewChooseSyncAPIs(apis map[string]bool) *cobra.Command {
	return &cobra.Command{
		Use:   "choose",
		Short: "Choose APIs to use",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if apis, ok := components.ChooseAPI(apis); ok {
				config.SetAPIs(apis)
				cmd.Println("APIs updated")
			} else {
				cmd.Println("API selection cancelled or failed")
			}
		},
	}
}

func NewSyncOnStartupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "startup",
		Short: "Manages automatic synchronization on application startup",
		Args:  cobra.NoArgs, // No direct arguments for 'startup' itself
	}
	cmd.AddCommand(NewSyncStartupEnableCmd())
	cmd.AddCommand(NewSyncStartupDisableCmd())
	return cmd
}

func NewSyncStartupEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Enables automatic synchronization when the application starts",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			config.SetSyncOnStartup(true)
			cmd.Println("Automatic sync on startup has been enabled.")
		},
	}
}

func NewSyncStartupDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disables automatic synchronization when the application starts",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			config.SetSyncOnStartup(false)
			cmd.Println("Automatic sync on startup has been disabled.")
		},
	}
}
