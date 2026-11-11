// Command server runs the candidate service.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/bootstrap"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "candidate:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return bootstrap.Run(ctx, cfg)
}
