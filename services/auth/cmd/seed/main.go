// Command seed creates the development users. It is idempotent: users that
// already exist are skipped.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/crypto"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/bootstrap"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/config"
)

const (
	seedTimeout = 2 * time.Minute
	//nolint:gosec // well-known development credential, never used outside local environments
	devPassword = "Password123!"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

func run() error {
	core, err := config.LoadCore()
	if err != nil {
		return err
	}
	sc, err := config.LoadSeed()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), seedTimeout)
	defer cancel()

	pool, err := postgres.NewPool(ctx, core.DBDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	// The issuer is unused by CreateUser, so none is supplied.
	svc, err := bootstrap.NewService(pool, crypto.NewHasher(core.Argon2Params()), nil, 0, log)
	if err != nil {
		return err
	}
	users := []struct {
		email, password string
		role            authz.Role
	}{
		{sc.AdminEmail, sc.AdminPassword, authz.RoleAdmin},
		{"candidate@smarthire.local", devPassword, authz.RoleCandidate},
		{"recruiter@smarthire.local", devPassword, authz.RoleRecruiter},
	}
	for _, u := range users {
		_, err := svc.CreateUser(ctx, u.email, u.password, u.role)
		switch {
		case err == nil:
			log.Info("seeded user", "role", string(u.role))
		case errors.Is(err, apperr.ErrConflict):
			log.Info("user exists, skipped", "role", string(u.role))
		default:
			return fmt.Errorf("seed %s: %w", u.role, err)
		}
	}
	return nil
}
