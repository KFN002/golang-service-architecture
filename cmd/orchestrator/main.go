// Command orchestrator runs the expression orchestrator service.
package main

import (
	"fmt"
	"os"

	"github.com/KFN002/perfect-go-service/config"
	"github.com/KFN002/perfect-go-service/internal/app"
)

func main() {
	if err := app.RunOrchestrator(config.LoadOrchestrator()); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: %v\n", err)
		os.Exit(1)
	}
}
