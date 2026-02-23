package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version is set at build time via ldflags:
//
//	go build -ldflags "-X github.com/florinutz/pgcdc/cmd.Version=v1.0.0"
var Version = "dev"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the pgcdc version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("pgcdc " + Version)
	},
}
