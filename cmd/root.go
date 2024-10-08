package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"go-oak-chunk/v2/vars"
)

var rootCmd = &cobra.Command{
	Use:  vars.AppName,
	Long: fmt.Sprintf("%s easily excute chunk dml to mysql like databases", vars.AppName),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Option missed! Use %s -h or --help for details.\n", vars.AppName)
	},
}

func initAll() {
	initVersion()
	initRun()
}

func Execute() {
	initAll()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
