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

// ReadChunk reads and validates a Netsy chunk file.
func ReadChunk(r io.Reader) ([]*Record, error) {
	kind := pb.FileKind_KIND_CHUNK
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

// WriteChunk writes records as a Netsy chunk file.
func WriteChunk(w io.Writer, records []*Record, leaderID string) error {
	protoRecords := make([]*pb.Record, 0, len(records))
	for _, record := range records {
		protoRecords = append(protoRecords, recordToProto(record))
	}

	writer, err := internaldatafile.NewWriterWithSmartCompression(
		bufio.NewWriter(w),
		pb.FileKind_KIND_CHUNK,
		protoRecords,
		leaderID,
	)
	if err != nil {
		return err
	}

	for i, record := range protoRecords {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("write record %d: %w", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}
