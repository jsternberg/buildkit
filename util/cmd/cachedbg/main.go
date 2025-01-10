package main

import (
	"os"

	"github.com/spf13/cobra"
)

var Command = &cobra.Command{
	Use: "cachedbg",
}

func main() {
	Command.AddCommand(FixCommand)

	if err := Command.Execute(); err != nil {
		os.Exit(1)
	}
}
