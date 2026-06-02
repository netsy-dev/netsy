// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/discovery"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBootstrapFirstElectorPromotesBootstrapSnapshotBeforeMembersFile(t *testing.T) {
	store := storage.NewMemoryStore()
	snapshotData := marshalElectorTestSnapshot(t, []*proto.Record{
		electorTestRecord(1),
		electorTestRecord(2),
	})
	if err := store.Put(context.Background(), datastore.BootstrapSnapshotKey, snapshotData); err != nil {
		t.Fatalf("Put(%s) error = %v", datastore.BootstrapSnapshotKey, err)
	}

	srv := newBootstrapTestServer(store)
	if err := srv.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}

	if _, err := discovery.ReadMembersFile(context.Background(), store); err != nil {
		t.Fatalf("ReadMembersFile() error = %v", err)
	}
	promoted, _, err := store.Get(context.Background(), datastore.SnapshotKey(2))
	if err != nil {
		t.Fatalf("Get(promoted snapshot) error = %v", err)
	}
	if !bytes.Equal(promoted, snapshotData) {
		t.Fatal("promoted snapshot data differs from bootstrap snapshot data")
	}
}

func TestBootstrapFirstElectorDoesNotWriteMembersFileWhenBootstrapSnapshotInvalid(t *testing.T) {
	store := storage.NewMemoryStore()
	_, chunkData, err := datastore.MarshalChunk(electorTestRecord(1), "node-a")
	if err != nil {
		t.Fatalf("MarshalChunk() error = %v", err)
	}
	if err := store.Put(context.Background(), datastore.BootstrapSnapshotKey, chunkData); err != nil {
		t.Fatalf("Put(%s) error = %v", datastore.BootstrapSnapshotKey, err)
	}

	srv := newBootstrapTestServer(store)
	if err := srv.Bootstrap(context.Background()); err == nil {
		t.Fatal("Bootstrap() succeeded with invalid bootstrap snapshot")
	}
	if _, err := discovery.ReadMembersFile(context.Background(), store); err == nil {
		t.Fatal("members.json was written despite invalid bootstrap snapshot")
	}
}

func newBootstrapTestServer(store storage.ObjectStorage) *Server {
	state := nodestate.New(slog.Default())
	return NewServer(
		slog.Default(),
		"test-cluster",
		store,
		state,
		time.Second,
		0,
		2,
		"", 0, nil, 0, 0, nil, nil, nil,
	)
}

func marshalElectorTestSnapshot(t *testing.T, records []*proto.Record) []byte {
	t.Helper()

	var buf bytes.Buffer
	writer, err := datafile.NewWriter(bufio.NewWriter(&buf), proto.FileKind_KIND_SNAPSHOT, int64(len(records)), "node-a")
	if err != nil {
		t.Fatalf("NewWriter(snapshot) error = %v", err)
	}
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Write(snapshot record %d) error = %v", record.GetRevision(), err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(snapshot writer) error = %v", err)
	}

	return buf.Bytes()
}

func electorTestRecord(revision int64) *proto.Record {
	return &proto.Record{
		Revision:       revision,
		Key:            []byte{byte('a' + revision)},
		Created:        true,
		Version:        1,
		CreateRevision: revision,
		CreatedAt:      timestamppb.New(time.Unix(revision, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte{byte('0' + revision)},
	}
}
