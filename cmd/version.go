package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version is set at build time via ldflags:
//
//	go build -ldflags "-X github.com/florinutz/pgpipe/cmd.Version=v1.0.0"
var Version = "dev"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the pgpipe version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("pgpipe " + Version)
	},
}
