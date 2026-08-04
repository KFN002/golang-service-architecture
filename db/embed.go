// Package db embeds the goose migration files into the binaries so each
// service migrates its own database on startup with zero external tooling.
package db

import "embed"

// MainMigrations holds the orchestrator database schema.
//
//go:embed main/migrations/*.sql
var MainMigrations embed.FS

// AuditMigrations holds the audit database schema.
//
//go:embed audit/migrations/*.sql
var AuditMigrations embed.FS
