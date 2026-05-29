// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datafile

import (
	"bufio"
	"fmt"
	"hash"
	"hash/crc64"
	"io"

	"github.com/klauspost/compress/zstd"
	pb "github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Writer struct {
	buffer        *bufio.Writer
	compressor    *zstd.Encoder
	recordWriter  io.Writer // Either compressor or buffer directly for records/footer
	hasher        hash.Hash64
	kind          pb.FileKind
	compression   pb.FileCompression
	recordsCount  int64
	firstRevision int64
	lastRevision  int64
	lastCount     int64
}

// NewWriter creates a data-file writer using the package's default compression selection for the given kind.
func NewWriter(buffer *bufio.Writer, kind pb.FileKind, recordsCount int64, leaderID string) (*Writer, error) {
	return NewWriterWithCompression(buffer, kind, recordsCount, leaderID, nil)
}

// NewWriterWithSmartCompression creates a writer that chooses compression from the record payload size for chunk files.
func NewWriterWithSmartCompression(buffer *bufio.Writer, kind pb.FileKind, records []*pb.Record, leaderID string) (*Writer, error) {
	var compression pb.FileCompression

	if kind == pb.FileKind_KIND_SNAPSHOT {
		// Always compress snapshots for internal Netsy use
		compression = pb.FileCompression_COMPRESSION_ZSTD
	} else {
		// For chunks, estimate size from key + value data
		totalSize := 0
		for _, record := range records {
			totalSize += len(record.Key) + len(record.Value)
		}

		// Use compression for chunks > 4KB
		if totalSize > 4096 {
			compression = pb.FileCompression_COMPRESSION_ZSTD
		} else {
			compression = pb.FileCompression_COMPRESSION_NONE
		}
	}

	return NewWriterWithCompression(buffer, kind, int64(len(records)), leaderID, &compression)
}

// NewWriterWithCompression creates a data-file writer with an optional explicit compression override.
func NewWriterWithCompression(buffer *bufio.Writer, kind pb.FileKind, recordsCount int64, leaderID string, forceCompression *pb.FileCompression) (*Writer, error) {
	// Determine compression type
	var compression pb.FileCompression
	if forceCompression != nil {
		compression = *forceCompression
	} else {
		// Smart compression logic
		if kind == pb.FileKind_KIND_SNAPSHOT {
			// Always compress snapshots for internal Netsy use
			compression = pb.FileCompression_COMPRESSION_ZSTD
		} else {
			// For chunks, default to no compression
			// Use NewWriterWithSmartCompression for size-based decisions
			compression = pb.FileCompression_COMPRESSION_NONE
		}
	}

	// Create writer
	w := &Writer{
		buffer:       buffer,
		hasher:       crc64.New(crcTable),
		kind:         kind,
		compression:  compression,
		recordsCount: recordsCount,
		lastCount:    0,
		lastRevision: 0,
	}

	// Create header (always uncompressed)
	header := &pb.FileHeader{
		Kind:         kind,
		RecordsCount: recordsCount,
		CreatedAt:    timestamppb.Now(),
		LeaderId:     leaderID,
		Compression:  compression,
		Crc:          0,
	}

	// Calculate header CRC
	headerData, err := proto.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal header: %w", err)
	}
	header.Crc = crc64.Checksum(headerData, crcTable)

	// Write header directly to buffer (always uncompressed)
	_, err = protodelim.MarshalTo(buffer, header)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal header: %s", err)
	}

	// Set up record writer based on compression type
	var compressor *zstd.Encoder
	var recordWriter io.Writer = buffer

	if compression == pb.FileCompression_COMPRESSION_ZSTD {
		// Create compressor for records and footer
		compressor, err = zstd.NewWriter(buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd compressor: %w", err)
		}
		recordWriter = compressor
	}

	w.compressor = compressor
	w.recordWriter = recordWriter

	return w, nil
}

func (w *Writer) Write(record *pb.Record) error {
	// Calculate record CRC
	record.Crc = 0
	data, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record %d: %w", w.lastCount, err)
	}
	record.Crc = crc64.Checksum(data, crcTable)

	// Write record to record writer
	_, err = protodelim.MarshalTo(w.recordWriter, record)
	if err != nil {
		return fmt.Errorf("failed to marshal record %d: %w", w.lastCount, err)
	}

	// Add zero CRC record data to CRC hash writer
	_, err = w.hasher.Write(data)
	if err != nil {
		return fmt.Errorf("failed to add record to records CRC: %s", err)
	}

	// Update first revision, last revision, and last count
	if w.firstRevision == 0 {
		w.firstRevision = record.Revision
	}
	w.lastCount++
	w.lastRevision = record.Revision

	return nil
}

func (w *Writer) Close() error {
	// Check last count matches expected count
	if w.lastCount != w.recordsCount {
		return fmt.Errorf("last count %d does not match expected count %d", w.lastCount, w.recordsCount)
	}

	// Calculate all records CRC and create footer
	footer := &pb.FileFooter{
		FirstRevision: w.firstRevision,
		LastRevision:  w.lastRevision,
		RecordsCrc:    w.hasher.Sum64(),
		Crc:           0,
	}

	// Calculate footer CRC
	footerData, err := proto.Marshal(footer)
	if err != nil {
		return fmt.Errorf("failed to marshal footer: %w", err)
	}
	footer.Crc = crc64.Checksum(footerData, crcTable)

	// Write footer to record writer
	_, err = protodelim.MarshalTo(w.recordWriter, footer)
	if err != nil {
		return fmt.Errorf("failed to marshal footer: %s", err)
	}

	// Close compressor if it exists (flushes and finalizes compression)
	if w.compressor != nil {
		err = w.compressor.Close()
		if err != nil {
			return fmt.Errorf("failed to close compressor: %w", err)
		}
	}

	// Flush underlying buffer
	return w.buffer.Flush()
}
