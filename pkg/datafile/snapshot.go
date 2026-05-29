// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datafile

import (
	"bufio"
	"fmt"
	"io"

	internaldatafile "github.com/netsy-dev/netsy/internal/datafile"
	pb "github.com/netsy-dev/netsy/internal/proto"
)

// ReadSnapshot reads and validates a Netsy snapshot file.
func ReadSnapshot(r io.Reader) ([]*Record, error) {
	kind := pb.FileKind_KIND_SNAPSHOT
	reader, err := internaldatafile.NewReader(bufio.NewReader(r), &kind)
	if err != nil {
		return nil, err
	}

	records := make([]*Record, 0, reader.Count())
	for i := int64(0); i < reader.Count(); i++ {
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		records = append(records, recordFromProto(record))
	}
	if _, err := reader.Close(); err != nil {
		return nil, err
	}

	return records, nil
}

// WriteSnapshot writes records as an uncompressed Netsy snapshot file.
func WriteSnapshot(w io.Writer, records []*Record, leaderID string) error {
	compression := pb.FileCompression_COMPRESSION_NONE
	writer, err := internaldatafile.NewWriterWithCompression(
		bufio.NewWriter(w),
		pb.FileKind_KIND_SNAPSHOT,
		int64(len(records)),
		leaderID,
		&compression,
	)
	if err != nil {
		return err
	}

	for i, record := range records {
		if err := writer.Write(recordToProto(record)); err != nil {
			return fmt.Errorf("write record %d: %w", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}
