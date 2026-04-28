package database

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RunMigrations(ctx context.Context, db *pgxpool.Pool) error {
	content, err := os.ReadFile("migrations/001_init.sql")
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, string(content))
	return err
}
