package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	grpcapi "github.com/NillHellberg/octocron/internal/api/grpc"
	"github.com/NillHellberg/octocron/internal/fsm"
	"github.com/NillHellberg/octocron/internal/scheduler"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

func main() {
	var (
		nodeID   = flag.String("node-id", "", "Unique node ID (e.g., octo1)")
		bindAddr = flag.String("bind-addr", "127.0.0.1:12000", "Raft bind address")
		grpcAddr = flag.String("grpc-addr", ":50051", "gRPC listen address")
		dataDir  = flag.String("data-dir", "./data", "Raft data directory")
		joinAddr = flag.String("join", "", "Address of an existing node to join (e.g., 10.0.0.98:50051)")
	)
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("--node-id is required")
	}

	store := fsm.NewJobStore()

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)

	addr, err := net.ResolveTCPAddr("tcp", *bindAddr)
	if err != nil {
		log.Fatal(err)
	}
	transport, err := raft.NewTCPTransport(*bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(*dataDir, 0700); err != nil {
		log.Fatal(err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(*dataDir, "raft-log.db"))
	if err != nil {
		log.Fatal(err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(*dataDir, "raft-stable.db"))
	if err != nil {
		log.Fatal(err)
	}

	snapStore, err := raft.NewFileSnapshotStore(*dataDir, 2, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	ra, err := raft.NewRaft(config, store, logStore, stableStore, snapStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	hasExistingState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		log.Fatal(err)
	}

	if *joinAddr != "" {
		if err := joinWithRedirect(*joinAddr, *nodeID, *bindAddr); err != nil {
			log.Fatalf("Failed to join cluster: %v", err)
		}
		log.Println("Successfully joined cluster")
	} else if !hasExistingState {
		bootstrapCfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(*nodeID),
					Address: raft.ServerAddress(*bindAddr),
				},
			},
		}
		f := ra.BootstrapCluster(bootstrapCfg)
		if err := f.Error(); err != nil {
			log.Fatalf("Bootstrap failed: %v", err)
		}
		log.Println("Bootstrapped single-node cluster")
	} else {
		log.Println("Existing cluster state found, skipping bootstrap")
	}

	sched := scheduler.NewScheduler(ra, store)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	octocron.RegisterOctocronServer(s, grpcapi.NewOctocronServer(sched, ra))
	reflection.Register(s)

	log.Printf("Octocron server started on %s (node %s)", *grpcAddr, *nodeID)
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func joinWithRedirect(joinAddr string, nodeID string, bindAddr string) error {
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to dial join node %s: %v", joinAddr, err)
		}
		client := octocron.NewOctocronClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Join(ctx, &octocron.JoinRequest{
			NodeId:      nodeID,
			RaftAddress: bindAddr,
		})
		cancel()
		conn.Close()

		if err == nil {
			return nil
		}

		st, ok := status.FromError(err)
		if ok && st.Code() == codes.FailedPrecondition && strings.Contains(st.Message(), "leader is at ") {
			parts := strings.SplitN(st.Message(), "leader is at ", 2)
			if len(parts) == 2 {
				leaderAddr := strings.TrimSpace(parts[1])
				// Временно: заменяем Raft-порт (12000) на gRPC-порт (50051)
				leaderAddr = strings.Replace(leaderAddr, ":12000", ":50051", 1)
				log.Printf("Not leader, retrying join at leader gRPC %s", leaderAddr)
				joinAddr = leaderAddr
				continue
			}
		}
		return fmt.Errorf("failed to join cluster: %v", err)
	}
	return fmt.Errorf("failed to join after %d attempts", maxRetries)
}
