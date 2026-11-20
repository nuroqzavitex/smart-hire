// Command projector keeps the OpenSearch job read model in step with job.*
// events, or rebuilds/reconciles it (ADR 0009):
//
//	projector                       consume job.* events
//	projector rebuild [--keep-old]  rebuild from Postgres and swap aliases
//	projector reconcile             compare open jobs; exit 2 on mismatch
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nuroqzavitex/smarthire/services/job/internal/bootstrap"
	"github.com/nuroqzavitex/smarthire/services/job/internal/config"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	cfg, err := config.LoadProjector()
	if err != nil {
		fmt.Fprintln(os.Stderr, "projector:", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmd := ""
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "":
		if err := bootstrap.RunProjector(ctx, cfg); err != nil {
			fmt.Fprintln(os.Stderr, "projector:", err)
			return 1
		}
	case "rebuild":
		fs := flag.NewFlagSet("rebuild", flag.ContinueOnError)
		keepOld := fs.Bool("keep-old", false, "keep the previous index after swapping aliases")
		if err := fs.Parse(args); err != nil {
			return 1
		}
		name, err := bootstrap.RunRebuild(ctx, cfg, *keepOld)
		if err != nil {
			fmt.Fprintln(os.Stderr, "rebuild:", err)
			return 1
		}
		fmt.Println("rebuilt read model into", name)
	case "reconcile":
		r, err := bootstrap.RunReconcile(ctx, cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "reconcile:", err)
			return 1
		}
		fmt.Printf("pg_open=%d index_open=%d diff=%v\n", r.PGOpen, r.IndexOpen, r.Diff)
		if !r.OK() {
			return 2
		}
	default:
		fmt.Fprintf(os.Stderr, "projector: unknown command %q (want rebuild or reconcile)\n", cmd)
		return 1
	}
	return 0
}
