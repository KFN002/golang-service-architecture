// Command agent runs one computation agent replica.
package main

import (
	"fmt"
	"os"

	// Sets GOMAXPROCS from the container CPU quota — without it a limited
	// container schedules against the host core count and thrashes.
	_ "go.uber.org/automaxprocs"

	"github.com/KFN002/perfect-go-service/config"
	"github.com/KFN002/perfect-go-service/internal/app"
)

func main() {
	if err := app.RunAgent(config.LoadAgent()); err != nil {
		fmt.Fprintf(os.Stderr, "agent: %v\n", err)
		os.Exit(1)
	}
}
