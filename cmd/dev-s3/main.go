// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Command dev-s3 runs a local fake S3 server for development.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3afero"
	"github.com/spf13/afero"
)

// s3Logger wraps a gofakes3.Logger to rewrite misleading log messages.
// gofakes3 logs "CREATE OBJECT" for PutObject; this rewrites it to "PUT OBJECT"
// to match S3 API terminology.
type s3Logger struct {
	inner gofakes3.Logger
}

func (l *s3Logger) Print(level gofakes3.LogLevel, v ...interface{}) {
	for i, val := range v {
		if s, ok := val.(string); ok {
			v[i] = strings.ReplaceAll(s, "CREATE OBJECT", "PUT OBJECT")
		}
	}
	l.inner.Print(level, v...)
}

func main() {
	var (
		addr   = flag.String("addr", ":4566", "Address to bind to")
		bucket = flag.String("bucket", "netsy-dev", "Default bucket to create")
		dir    = flag.String("dir", "temp/dev-s3", "Directory to store S3 files")
	)
	flag.Parse()

	absDir, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatalf("resolving directory: %v", err)
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		log.Fatalf("creating directory %s: %v", absDir, err)
	}

	fs := afero.NewBasePathFs(afero.NewOsFs(), absDir)
	metaFs := afero.NewBasePathFs(afero.NewOsFs(), absDir)
	backend, err := s3afero.SingleBucket(*bucket, fs, metaFs)
	if err != nil {
		log.Fatalf("creating S3 backend: %v", err)
	}

	faker := gofakes3.New(backend,
		gofakes3.WithAutoBucket(true),
		gofakes3.WithLogger(&s3Logger{inner: gofakes3.GlobalLog()}),
	)

	srv := &http.Server{
		Addr:    *addr,
		Handler: faker.Server(),
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		fmt.Println("\nshutting down dev-s3...")
		srv.Close()
	}()

	log.Printf("dev-s3: listening on %s (bucket=%s, dir=%s)", *addr, *bucket, absDir)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
