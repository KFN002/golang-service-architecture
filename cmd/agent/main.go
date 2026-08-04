// Command agent runs one computation agent replica.
package main

import (
	"fmt"
	"os"

	"github.com/KFN002/perfect-go-service/config"
	"github.com/KFN002/perfect-go-service/internal/app"
)

func main() {
	if err := app.RunAgent(config.LoadAgent()); err != nil {
		fmt.Fprintf(os.Stderr, "agent: %v\n", err)
		os.Exit(1)
	}
}
