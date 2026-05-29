// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"sync"

	"github.com/netsy-dev/netsy/internal/proto"
)

// FailingDB wraps a Database implementation and can be toggled to cause
// Commit failures on transactions. When failCommit is enabled, InsertRecord
// succeeds but the underlying sql.Tx is immediately rolled back so that the
// subsequent Commit call returns an error. This is used for example to
// simulate the scenario where object storage writes succeed but the local
// SQLite commit does not.
type FailingDB struct {
	inner      Database
	mu         sync.RWMutex
	failCommit bool
}

// NewFailingDB returns a FailingDB that delegates to inner.
func NewFailingDB(inner Database) *FailingDB {
	return &FailingDB{inner: inner}
}

// SetFailCommit toggles whether the next transaction commit will fail.
func (f *FailingDB) SetFailCommit(fail bool) {
	f.mu.Lock()
	f.failCommit = fail
	f.mu.Unlock()
}

func (f *FailingDB) shouldFailCommit() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.failCommit
}

func (f *FailingDB) Connect() error                 { return f.inner.Connect() }
func (f *FailingDB) LatestRevision() (int64, error) { return f.inner.LatestRevision() }
func (f *FailingDB) RecordCount() (int64, error)    { return f.inner.RecordCount() }
func (f *FailingDB) VerifyIntegrity() error         { return f.inner.VerifyIntegrity() }
func (f *FailingDB) Truncate() error                { return f.inner.Truncate() }
func (f *FailingDB) LatestCompactionRevision() (int64, error) {
	return f.inner.LatestCompactionRevision()
}
func (f *FailingDB) DeriveCompactionRevision() (int64, error) {
	return f.inner.DeriveCompactionRevision()
}
func (f *FailingDB) PersistCompactionRevision(revision int64) error {
	return f.inner.PersistCompactionRevision(revision)
}
func (f *FailingDB) ExecuteCompaction(compactionRevision int64) (int64, error) {
	return f.inner.ExecuteCompaction(compactionRevision)
}
func (f *FailingDB) Size() (int64, error)      { return f.inner.Size() }
func (f *FailingDB) SizeInUse() (int64, error) { return f.inner.SizeInUse() }
func (f *FailingDB) Close() error              { return f.inner.Close() }
func (f *FailingDB) BeginTx() (*Tx, error)     { return f.inner.BeginTx() }

func (f *FailingDB) GetRevision(findRevision int64) (int64, bool, sql.NullString, error) {
	return f.inner.GetRevision(findRevision)
}

func (f *FailingDB) FindRecordsBy(whereQuery string, whereArgs []any, revision int64, limit int64, order string) ([]*proto.Record, int64, int64, error) {
	return f.inner.FindRecordsBy(whereQuery, whereArgs, revision, limit, order)
}

func (f *FailingDB) FindRecordByRev(revision int64) (*proto.Record, error) {
	return f.inner.FindRecordByRev(revision)
}

func (f *FailingDB) FindAllRecordsForSnapshot(upToRevision int64) ([]*proto.Record, error) {
	return f.inner.FindAllRecordsForSnapshot(upToRevision)
}

func (f *FailingDB) FindRecordsAfterRevision(revision int64) ([]*proto.Record, error) {
	return f.inner.FindRecordsAfterRevision(revision)
}

func (f *FailingDB) ReplicateRecord(record *proto.Record) (*proto.Record, error) {
	return f.inner.ReplicateRecord(record)
}

func (f *FailingDB) ReplicateTentativeRecord(record *proto.Record, committedRevision int64) (*proto.Record, error) {
	return f.inner.ReplicateTentativeRecord(record, committedRevision)
}

// InsertRecord delegates to the inner database. When failCommit is enabled,
// the insert succeeds but the underlying sql.Tx is immediately rolled back
// and nilled so the subsequent Commit call returns an error.
func (f *FailingDB) InsertRecord(record *proto.Record, tx *Tx) (*proto.Record, error) {
	result, err := f.inner.InsertRecord(record, tx)
	if err == nil && f.shouldFailCommit() {
		if tx.tx != nil {
			_ = tx.tx.Rollback()
			tx.tx = nil
		}
	}
	return result, err
}
