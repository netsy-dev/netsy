// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datafile

import (
	"bufio"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"

	"github.com/klauspost/compress/zstd"
	pb "github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

type Reader struct {
	buffer               *bufio.Reader
	decompressor         *zstd.Decoder
	reader               *bufio.Reader // Either decompressed or raw buffer
	hasher               hash.Hash64
	kind                 pb.FileKind
	compression          pb.FileCompression
	expectedRecordsCount int64
	firstRevision        int64
	lastRevision         int64
	lastCount            int64
}

type ReadResults struct {
	Kind          string
	RecordsCount  int64
	FirstRevision int64
	LastRevision  int64
}

// NewReader opens a Netsy data file reader, validates the header, and prepares any decompression needed for records.
func NewReader(buffer *bufio.Reader, expectKind *pb.FileKind) (*Reader, error) {
	// Always read header uncompressed first
	var header pb.FileHeader
	err := protodelim.UnmarshalFrom(buffer, &header)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Check kind matches expected
	if expectKind != nil && *expectKind != header.Kind {
		return nil, fmt.Errorf("expected kind mismatch - expected %d, got %d", expectKind, header.Kind)
	}

	// Validate compression type
	if header.Compression == pb.FileCompression_COMPRESSION_UNKNOWN {
		return nil, fmt.Errorf("unknown compression type in header")
	}

	// Calculate header CRC
	headerClone, ok := proto.Clone(&header).(*pb.FileHeader)
	if !ok {
		return nil, fmt.Errorf("failed to clone header")
	}
	headerClone.Crc = 0
	headerData, err := proto.Marshal(headerClone)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal header: %w", err)
	}
	actualCrc := crc64.Checksum(headerData, crcTable)

	// Verify header CRC
	if header.Crc != actualCrc {
		return nil, fmt.Errorf("header CRC %d mismatch - expected %d", actualCrc, header.Crc)
	}

	// Set up record reader based on compression type from header
	var decompressor *zstd.Decoder
	var recordReader io.Reader = buffer

	if header.Compression == pb.FileCompression_COMPRESSION_ZSTD {
		// Records and footer are compressed
		decompressor, err = zstd.NewReader(buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decompressor: %w", err)
		}
		recordReader = decompressor
	}
	// If COMPRESSION_NONE, continue reading directly from buffer

	// Return a reader
	return &Reader{
		buffer:               buffer,
		decompressor:         decompressor,
		reader:               bufio.NewReader(recordReader),
		hasher:               crc64.New(crcTable),
		kind:                 header.Kind,
		compression:          header.Compression,
		expectedRecordsCount: header.RecordsCount,
	}, nil
}

func (r *Reader) Count() int64 {
	return r.expectedRecordsCount
}

// TODO: change Read to an iterator we can just loop on
func (r *Reader) Read() (record *pb.Record, err error) {
	record = &pb.Record{}
	// Read record from reader (either compressed or uncompressed)
	err = protodelim.UnmarshalFrom(r.reader, record)
	if errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("unexpected end of file")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal record %d: %w", r.lastCount, err)
	}

	// Calculate record CRC
	recordClone, ok := proto.Clone(record).(*pb.Record)
	if !ok {
		return nil, fmt.Errorf("failed to clone record")
	}
	recordClone.Crc = 0
	recordData, err := proto.Marshal(recordClone)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal record: %w", err)
	}
	actualCrc := crc64.Checksum(recordData, crcTable)

	// Verify record CRC
	if record.Crc != actualCrc {
		return nil, fmt.Errorf("record %d CRC %d mismatch - expected %d", r.lastCount, actualCrc, record.Crc)
	}

	// Add to records CRC calculation
	_, err = r.hasher.Write(recordData)
	if err != nil {
		return nil, fmt.Errorf("failed to add record to CRC: %w", err)
	}

	// Update first revision, last revision, and last count
	if r.firstRevision == 0 {
		r.firstRevision = record.Revision
	}
	r.lastCount++
	r.lastRevision = record.Revision

	// Return record
	return record, nil
}

func (r *Reader) Close() (results ReadResults, err error) {
	// Check last count matches expected records count from header
	if r.lastCount != r.expectedRecordsCount {
		return ReadResults{}, fmt.Errorf("last count %d does not match expected count %d", r.lastCount, r.expectedRecordsCount)
	}

	// Read footer from reader (either compressed or uncompressed)
	var footer pb.FileFooter
	err = protodelim.UnmarshalFrom(r.reader, &footer)
	if err != nil {
		return ReadResults{}, fmt.Errorf("failed to unmarshal footer: %w", err)
	}

	// Calculate footer CRC
	footerClone, ok := proto.Clone(&footer).(*pb.FileFooter)
	if !ok {
		return ReadResults{}, fmt.Errorf("failed to clone footer")
	}
	footerClone.Crc = 0
	footerData, err := proto.Marshal(footerClone)
	if err != nil {
		return ReadResults{}, fmt.Errorf("failed to marshal footer: %w", err)
	}
	actualCrc := crc64.Checksum(footerData, crcTable)

	// Verify footer CRC
	if footer.Crc != actualCrc {
		return ReadResults{}, fmt.Errorf("footer CRC %d mismatch - expected %d", actualCrc, footer.Crc)
	}

	// Calculate records CRC
	recordsCrc := r.hasher.Sum64()

	// Verify records CRC
	if footer.RecordsCrc != recordsCrc {
		return ReadResults{}, fmt.Errorf("records CRC %d mismatch - expected %d", recordsCrc, footer.RecordsCrc)
	}

	// Check first revision matches expected first revision
	if r.firstRevision != footer.FirstRevision {
		return ReadResults{}, fmt.Errorf("first revision %d does not match expected first revision %d", r.firstRevision, footer.FirstRevision)
	}

	// Check last revision matches expected last revision
	if r.lastRevision != footer.LastRevision {
		return ReadResults{}, fmt.Errorf("last revision %d does not match expected last revision %d", r.lastRevision, footer.LastRevision)
	}

	return ReadResults{
		Kind:          r.kind.String(),
		RecordsCount:  r.lastCount,
		FirstRevision: r.firstRevision,
		LastRevision:  r.lastRevision,
	}, nil
}
