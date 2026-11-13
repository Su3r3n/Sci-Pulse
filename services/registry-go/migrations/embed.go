package migrations

import "embed"

// FS embeds SQL migration files for golang-migrate.
//
//go:embed *.sql
var FS embed.FS
