// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"

	"github.com/netsy-dev/netsy/internal/bootstrap"
	"github.com/netsy-dev/netsy/internal/buildvars"
	"github.com/netsy-dev/netsy/internal/clientapi"
	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/discovery"
	"github.com/netsy-dev/netsy/internal/elector"
	"github.com/netsy-dev/netsy/internal/healthserver"
	"github.com/netsy-dev/netsy/internal/heartbeat"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/mtls"
	"github.com/netsy-dev/netsy/internal/node"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/primary"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/replication"
	"github.com/netsy-dev/netsy/internal/snapshot"
	"github.com/netsy-dev/netsy/internal/storage"
	"github.com/netsy-dev/netsy/internal/watch"
	"github.com/spf13/cobra"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var (
	flagConfig   string
	flagValidate string
	flagVerbose  bool
	flagVersion  bool
)

var rootCmd = &cobra.Command{
	Use:   "netsy",
	Short: "Netsy",
	Long:  `netsy is an etcd alternative which implements a subset of the etcd API for use with for Kubernetes.`,
}

func init() {
	pflags := rootCmd.PersistentFlags()
	pflags.BoolVarP(&flagVerbose, "verbose", "v", false, "Enable verbose output")
	pflags.BoolVar(&flagVersion, "version", false, "Show version information")
	pflags.StringVar(&flagConfig, "config", "", "Path to JSONC config file (overrides NETSY_CONFIG env var)")
	pflags.StringVar(&flagValidate, "validate", "", "Validate a config file and exit")
}

// NewRootCmd constructs the netsy CLI root command and wires startup behavior into its Run function.
func NewRootCmd() *cobra.Command {
	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	}))

	// Define root command
	rootCmd.Run = func(cmd *cobra.Command, args []string) {
		var err error

		// Capture start time early for Primary leader election tie-breaking.
		startTime := time.Now().Unix()

		// Register signal handler early so signals during startup are
		// captured in the buffered channel and handled once we reach
		// the select loop.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		// Define flags which can be used to override env vars
		flags := config.FlagOverrides{
			ConfigPath: flagConfig,
			Verbose:    flagVerbose,
		}

		// Check for version flag
		if flagVersion {
			fmt.Printf("netsy %s\n", buildvars.BuildVersion())
			if flagVerbose {
				fmt.Printf("build version: %s\n", buildvars.BuildVersion())
				fmt.Printf("build date: %s\n", buildvars.BuildDate())
				fmt.Printf("commit hash: %s\n", buildvars.CommitHash())
				fmt.Printf("commit date: %s\n", buildvars.CommitDate())
				fmt.Printf("commit branch: %s\n", buildvars.CommitBranch())
			}
			return
		}

		// Handle --validate: load + validate config file, print result, exit
		if flagValidate != "" {
			if err := config.ValidateFile(flagValidate, flags); err != nil {
				fmt.Printf("Validation failed: %v\n", err)
				os.Exit(1)
			}
			fmt.Println("Config is valid.")
			return
		}

		// Load and validate config
		c, err := config.Load(flags)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}

		// Apply log level filtering based on verbose setting
		filteredLogger := logger
		if c.Verbose {
			filteredLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				AddSource: true,
				Level:     slog.LevelDebug,
			}))
		}

		// Log modes
		if c.Verbose {
			fmt.Println("Verbose output ENABLED")
		}

		// Add cluster_id and node_id as default attributes on all log entries.
		filteredLogger = filteredLogger.With("cluster_id", c.ClusterID, "node_id", c.NodeID)

		// Initialise node state (Loading / Follower / Replica)
		state := nodestate.New(filteredLogger)

		// Create Prometheus registry and always-on metrics.
		quorumLabel := "disabled"
		if c.Replication.Quorum != nil {
			switch q := *c.Replication.Quorum; {
			case q == -1:
				quorumLabel = "majority"
			case q > 0:
				quorumLabel = fmt.Sprintf("static:%d", q)
			}
		}
		reg := metrics.NewRegistry()
		stateMetrics := nodestate.NewStateMetrics(buildvars.BuildVersion(), c.ClusterID, c.NodeID, quorumLabel)
		stateMetrics.ProcessStartTime.Set(float64(startTime))
		state.SetMetrics(stateMetrics)
		retryMetrics := metrics.NewRetryMetrics()
		compactionMetrics := metrics.NewCompactionMetrics()
		for _, c := range stateMetrics.Collectors() {
			reg.MustRegister(c)
		}
		for _, c := range retryMetrics.Collectors() {
			reg.MustRegister(c)
		}
		for _, c := range compactionMetrics.Collectors() {
			reg.MustRegister(c)
		}

		// Create role groups for Primary, Elector, and Replica metrics.
		primaryGroup := metrics.NewRoleGroup(func() bool {
			ps := state.Primary()
			return ps == nodestate.PrimaryStarting || ps == nodestate.PrimaryActive || ps == nodestate.PrimaryDraining
		})
		electorGroup := metrics.NewRoleGroup(func() bool {
			return state.Elector() == nodestate.ElectorLeader
		})
		replicaGroup := metrics.NewRoleGroup(func() bool {
			return state.Primary() == nodestate.PrimaryReplica
		})
		reg.MustRegister(primaryGroup)
		reg.MustRegister(electorGroup)
		reg.MustRegister(replicaGroup)

		// Create per-package metrics and register them with role groups.
		primaryMetrics := primary.NewMetrics()
		primaryGroup.Add(primaryMetrics.Collectors()...)
		electorMetrics := elector.NewMetrics()
		electorGroup.Add(electorMetrics.Collectors()...)
		clientMetrics := clientapi.NewMetrics()
		for _, col := range clientMetrics.AlwaysOnCollectors() {
			reg.MustRegister(col)
		}
		replicaMetrics := replication.NewMetrics()
		replicaGroup.Add(replicaMetrics.Collectors()...)
		replicaGroup.Add(clientMetrics.ReplicaCollectors()...)
		storageMetrics := metrics.NewObjectStorageMetrics()
		for _, col := range storageMetrics.Collectors() {
			reg.MustRegister(col)
		}
		bootstrapMetrics := bootstrap.NewMetrics()
		for _, col := range bootstrapMetrics.Collectors() {
			reg.MustRegister(col)
		}

		// Register gRPC interceptor metrics shared by both Client and Peer servers.
		grpcMetrics := grpc_prometheus.NewServerMetrics()
		grpcMetrics.EnableHandlingTimeHistogram()
		reg.MustRegister(grpcMetrics)

		// Start HTTP health server with /metrics endpoint.
		healthSrv, err := healthserver.New(filteredLogger, c.BindHealth, state, reg)
		if err != nil {
			filteredLogger.Error("Failed to start health server", "error", err)
			os.Exit(1)
		}
		healthSrv.Start()
		defer healthSrv.Close()
		filteredLogger.Info("starting health (http) server...", "addr", c.BindHealth)

		// Load certs and keys
		tlsFiles, err := config.LoadTLSFiles(c)
		if err != nil {
			filteredLogger.Error("Failed to load TLS files", "err", err)
			jitterWaitThenExit(filteredLogger)
		}
		if err := mtls.ValidateLocalNodeCertificates(c, tlsFiles); err != nil {
			filteredLogger.Error("Invalid local TLS certificates", "err", err)
			jitterWaitThenExit(filteredLogger)
		}
		tlsConfigClientAPI, err := mtls.NewServerTLSConfig(c, tlsFiles, mtls.RoleClient)
		if err != nil {
			filteredLogger.Error("Failed to construct client API TLS config", "err", err)
			jitterWaitThenExit(filteredLogger)
		}
		tlsConfigPeerAPI, err := mtls.NewServerTLSConfig(c, tlsFiles, mtls.RolePeer)
		if err != nil {
			filteredLogger.Error("Failed to construct peer API TLS config", "err", err)
			jitterWaitThenExit(filteredLogger)
		}
		tlsConfigPeerClient, err := mtls.NewClientTLSConfig(tlsFiles)
		if err != nil {
			filteredLogger.Error("Failed to construct peer client TLS config", "err", err)
			jitterWaitThenExit(filteredLogger)
		}

		// Create root context for all background services
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create object storage client.
		storageClient, storageCleanup, err := storage.New(c, filteredLogger)
		if err != nil {
			filteredLogger.Error("Failed to create storage client", "error", err)
			os.Exit(1)
		}
		defer storageCleanup()

		// Instantiate database
		db := localdb.New(fmt.Sprintf("%s/db.sqlite3", c.DataDir))
		err = db.Connect()
		if err != nil {
			filteredLogger.Error("db.Connect error", "error", err)
			jitterWaitThenExit(filteredLogger)
		}
		filteredLogger.Info("local_db_initialized", "path", fmt.Sprintf("%s/db.sqlite3", c.DataDir))

		// Construct the snapshot worker early so the Primary server can hold a
		// stable reference to it during bootstrap and later role transitions.
		snapshotMetrics := snapshot.NewMetrics()
		primaryGroup.Add(snapshotMetrics.Collectors()...)
		snapshotWorker := snapshot.NewWorker(filteredLogger, c, db, storageClient, snapshotMetrics, storageMetrics)
		defer snapshotWorker.Stop()

		// watchManager is shared by the Client API server (watch event
		// distribution), Primary (compaction watch-admission gating), and
		// Node server (min watch revision queries).
		watchManager := watch.NewManager(filteredLogger.With("component", "watch"), db)

		// Create peer manager for outbound connections. Constructed before the
		// Primary server because the compaction scheduler dials each node's
		// Node service to query minimum watch revisions.
		peerManager := peerclient.NewManager(
			filteredLogger.With("component", "peer-manager"),
			c.NodeID,
			tlsConfigPeerClient,
			state,
		)
		defer peerManager.Close()

		// Construct the Primary server before bootstrap so the same instance can
		// be wired into peer/client services now and activated later on election.
		primarySrv, err := primary.NewServer(
			filteredLogger.With("component", "primary"),
			c,
			db,
			snapshotWorker,
			storageClient,
			state,
			peerManager,
			watchManager,
			primaryMetrics,
			c.HeartbeatInterval.Duration,
			c.Replication.DegradationCount,
			compactionMetrics,
			retryMetrics,
			storageMetrics,
		)
		if err != nil {
			filteredLogger.Error("Failed to create primary server", "error", err)
			os.Exit(1)
		}

		// Start elector — s3lect election, health server, and elector service.
		// Created after DB and peer manager so all election dependencies are
		// available at construction time, and before the client API server so
		// the Elector server can be wired in as the local member lister.
		electionRunner, err := elector.New(
			filteredLogger, c, state, storageClient, tlsFiles.ServerCert,
			startTime, db, peerManager, retryMetrics,
		)
		if err != nil {
			filteredLogger.Error("Failed to create election runner", "error", err)
			jitterWaitThenExit(filteredLogger)
		}
		electionRunner.SetMetrics(electorMetrics)
		if err := electionRunner.Start(ctx); err != nil {
			filteredLogger.Error("Failed to start election runner", "error", err)
			jitterWaitThenExit(filteredLogger)
		}
		defer electionRunner.Stop(ctx)
		filteredLogger.Info("starting election (https) server...", "addr", c.BindElection)

		// Construct the client API server before bootstrap so its watch
		// notifier can be used by the replication follower, but do not
		// start serving client traffic until bootstrap completes.
		gopts := []grpc.ServerOption{
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             embed.DefaultGRPCKeepAliveMinTime,
				PermitWithoutStream: false,
			}),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    embed.DefaultGRPCKeepAliveInterval,
				Timeout: embed.DefaultGRPCKeepAliveTimeout,
			}),
			grpc.StreamInterceptor(grpcMetrics.StreamServerInterceptor()),
			grpc.UnaryInterceptor(grpcMetrics.UnaryServerInterceptor()),
		}
		gopts = append(gopts, grpc.Creds(credentials.NewTLS(tlsConfigClientAPI)))
		grpcServer := grpc.NewServer(gopts...)
		clientApiServer := clientapi.NewServer(filteredLogger, c, db, grpcServer, primarySrv, peerManager, watchManager, state, electionRunner.ElectorServer(), clientMetrics)

		// Wait for first election cycle to know the current Elector
		electionStatus, err := electionRunner.WaitForFirstElection(ctx)
		if err != nil {
			filteredLogger.Error("Failed to complete first elector leader election cycle", "error", err)
			jitterWaitThenExit(filteredLogger)
		}
		filteredLogger.Info("first elector leader election cycle complete",
			"is_leader", electionStatus.IsLeader,
			"leader_id", electionStatus.LeaderID,
			"leader_addr", electionStatus.LeaderAddr,
		)

		// s3lect returns the Elector's election-health address; peer RPCs must
		// use the node's peer advertise address from service discovery instead.
		electorPeerAddr := c.AdvertisePeer
		if electionStatus.LeaderID != c.NodeID {
			reg, err := discovery.ReadNodeRegistration(ctx, storageClient, electionStatus.LeaderID)
			if err != nil {
				filteredLogger.Error("Failed to resolve elector peer address from service discovery",
					"leader_id", electionStatus.LeaderID,
					"error", err,
				)
				jitterWaitThenExit(filteredLogger)
			}
			electorPeerAddr = reg.PeerAdvertiseAddress
		}

		// Wire local Elector server for self-elector heartbeat delivery.
		peerManager.SetLocalElectorServer(electionRunner.ElectorServer())

		// Connect to the current Elector for peer RPCs
		if err := peerManager.ConnectElector(electionStatus.LeaderID, electorPeerAddr); err != nil {
			filteredLogger.Error("Failed to connect to elector", "error", err)
			jitterWaitThenExit(filteredLogger)
		}

		// Create heartbeat sender (started after follower is constructed
		// so the self-degradation callback can reference it).
		heartbeatSender := heartbeat.NewSender(
			filteredLogger.With("component", "heartbeat"),
			c.NodeID,
			state,
			peerManager,
			db,
			startTime,
			c.HeartbeatInterval.Duration,
			retryMetrics,
		)

		// Create the replication follower.
		follower := replication.NewFollower(
			filteredLogger.With("component", "follower"),
			c.NodeID,
			state,
			peerManager,
			db,
			heartbeatSender,
			watchManager,
			replicaMetrics,
			compactionMetrics,
			retryMetrics,
		)

		// Wire Primary self-degradation: when the Primary's elector
		// heartbeat fails, send a signal to trigger the normal shutdown
		// path which will drain, flush, deregister, and exit. The process
		// restarting will likely lead to it becoming a Replica.
		heartbeatSender.SetPrimarySelfDegradeFunc(func() {
			filteredLogger.Warn("primary self-degradation triggered, initiating shutdown")
			select {
			case sigCh <- syscall.SIGTERM:
			default:
			}
		})

		go heartbeatSender.Run(ctx)

		// Complete node loading and backfill before serving client traffic.
		bootstrapResult, err := bootstrap.Run(
			ctx,
			filteredLogger.With("component", "bootstrap"),
			c,
			state,
			db,
			storageClient,
			peerManager,
			electionRunner,
			follower,
			bootstrapMetrics,
			storageMetrics,
		)
		if err != nil {
			filteredLogger.Error("bootstrap failed", "error", err)
			jitterWaitThenExit(filteredLogger)
		}
		snapshotWorker.InitializeWithSnapshot(bootstrapResult.LatestSnapshotInfo)
		snapshotWorker.Start()

		// Create Node gRPC server
		nodeSrv := node.NewServer(
			filteredLogger.With("component", "node-server"),
			c.NodeID,
			startTime,
			state,
			db,
			peerManager,
			watchManager,
		)

		// Setup and run Peer gRPC server (Elector, Primary, and Node services)
		peerOpts := []grpc.ServerOption{
			grpc.Creds(credentials.NewTLS(tlsConfigPeerAPI)),
			grpc.StreamInterceptor(grpcMetrics.StreamServerInterceptor()),
			grpc.UnaryInterceptor(grpcMetrics.UnaryServerInterceptor()),
		}
		peerGRPCServer := grpc.NewServer(peerOpts...)
		proto.RegisterElectorServer(peerGRPCServer, electionRunner.ElectorServer())
		proto.RegisterPrimaryServer(peerGRPCServer, primarySrv)
		proto.RegisterNodeServer(peerGRPCServer, nodeSrv)
		pb.RegisterKVServer(peerGRPCServer, primary.NewPeerKVServer(clientApiServer.ApplyTxn))
		peerListener, err := net.Listen("tcp", c.BindPeer)
		if err != nil {
			filteredLogger.Error("Unable to create peer gRPC server listener", "err", err)
			os.Exit(1)
		}
		filteredLogger.Info("starting peer (grpc) server...", "addr", c.BindPeer)
		peerShutdownCh := make(chan error, 1)
		go func() {
			peerShutdownCh <- peerGRPCServer.Serve(peerListener)
		}()

		// Run gRPC server with the etcd-compatible client API after the
		// node has completed bootstrap and become Healthy.
		grpcListener, err := net.Listen("tcp", c.BindClient)
		if err != nil {
			filteredLogger.Error("Unable to create gRPC server listener", "err", err)
			os.Exit(1)
		}
		filteredLogger.Info("starting client (grpc) server...", "addr", c.BindClient)
		shutdownCh := make(chan error, 1)
		go func() {
			shutdownCh <- grpcServer.Serve(grpcListener)
		}()

		// Wire role change hooks so the follower and primary services
		// are started and stopped on leadership transitions.
		peerManager.SetPrimaryChangeFunc(func(isPrimary bool) {
			if isPrimary {
				follower.Stop()
				primarySrv.StartServices(ctx)
				electionRunner.ElectorServer().SetHeartbeatForwarder(primarySrv)
			} else {
				electionRunner.ElectorServer().SetHeartbeatForwarder(nil)
				primarySrv.StopServices()
				if err := follower.Start(ctx); err != nil {
					filteredLogger.Error("failed to start follower", "error", err)
				}
			}
		})

		// Bootstrap may already have established that this node is the current
		// Primary, so start Primary services immediately in that case.
		if state.ClusterState().Primary.NodeID == c.NodeID {
			primarySrv.StartServices(ctx)
			electionRunner.ElectorServer().SetHeartbeatForwarder(primarySrv)
		}

		// Block until SIGTERM/SIGINT or gRPC server error
		select {
		case sig := <-sigCh:
			filteredLogger.Info("received signal, starting shutdown", "signal", sig.String())
		case err := <-shutdownCh:
			filteredLogger.Error("client grpc server failed, starting shutdown", "error", err)
		case err := <-peerShutdownCh:
			filteredLogger.Error("peer grpc server failed, starting shutdown", "error", err)
		}
		signal.Stop(sigCh)

		// If this node is the Primary, drain writes and flush the chunk
		// buffer to object storage before proceeding with deregistration.
		if state.Primary() != nodestate.PrimaryReplica {
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := primarySrv.GracefulDrain(drainCtx); err != nil {
				filteredLogger.Error("primary graceful drain failed", "error", err)
			}
			drainCancel()
		}

		cancel() // stop heartbeat sender and other background goroutines

		// Mark node as degraded health
		if err := state.SetHealth(nodestate.HealthDegraded); err != nil {
			filteredLogger.Error("failed to transition health to degraded", "error", err)
		}

		// Create a timeout for deregistration
		deregCtx, deregCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer deregCancel()

		// Deregister node with Elector
		if ec := peerManager.ElectorClient(); ec != nil {
			if _, err := ec.DeregisterNode(deregCtx, &proto.DeregisterNodeRequest{NodeId: c.NodeID}); err != nil {
				filteredLogger.Error("deregistration RPC failed", "error", err)
			}
		} else {
			filteredLogger.Warn("no elector connection available for deregistration")
		}

		// Delete node registration file in object storage
		if err := discovery.DeleteNodeRegistration(deregCtx, storageClient, c.NodeID); err != nil {
			filteredLogger.Error("failed to delete node registration file", "error", err)
		}

		// Close API servers and exit
		clientApiServer.Close()
		peerGRPCServer.GracefulStop()
		filteredLogger.Info("exiting")
	}

	return rootCmd
}

func jitterWaitThenExit(logger *slog.Logger) {
	waitFor := time.Duration(rand.Intn(10)) * time.Second
	logger.Info("waiting before exiting", "wait", waitFor)
	time.Sleep(waitFor)
	logger.Info("exiting...")
	os.Exit(1)
}
