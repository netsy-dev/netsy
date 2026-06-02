// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"fmt"
)

// Tx represents a database transaction, similar to database/sql.Tx
type Tx struct {
	tx *sql.Tx
	db *database
}

// BeginTx starts a new transaction
func (db *database) BeginTx() (*Tx, error) {
	sqlTx, err := db.conn.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &Tx{
		tx: sqlTx,
		db: db,
	}, nil
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.tx == nil {
		return fmt.Errorf("transaction already committed or rolled back")
	}

	err := tx.tx.Commit()
	tx.tx = nil // Mark as completed
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	if tx.tx == nil {
		return nil // Already completed
	}

	err := tx.tx.Rollback()
	tx.tx = nil // Mark as completed
	return err
}
