package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	grpcapi "github.com/NillHellberg/octocron/internal/api/grpc"
	"github.com/NillHellberg/octocron/internal/scheduler"
	"github.com/NillHellberg/octocron/internal/fsm"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed web/index.html
var webIndex []byte

func main() {
	var (
		nodeID   = flag.String("node-id", "", "Unique node ID (e.g., octo1)")
		bindAddr = flag.String("bind-addr", "127.0.0.1:12000", "Raft bind address")
		grpcAddr = flag.String("grpc-addr", ":50051", "gRPC listen address")
		httpPort = flag.String("http-port", "8080", "HTTP port for web interface")
		dataDir  = flag.String("data-dir", "./data", "Raft data directory")
		joinAddr = flag.String("join", "", "Address of an existing node to join")
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

	// gRPC сервер
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	octocron.RegisterOctocronServer(grpcServer, grpcapi.NewOctocronServer(sched, ra))
	reflection.Register(grpcServer)
	go func() {
		log.Printf("gRPC server started on %s", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// HTTP сервер (веб-интерфейс и REST API)
	go func() {
		mux := http.NewServeMux()

		mux.HandleFunc("/api/jobs", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				listJobsHTTP(w, r, sched)
			case http.MethodPost:
				createJobHTTP(w, r, sched)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		})
		mux.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/jobs/"), "/")
			if len(parts) == 0 || parts[0] == "" {
				http.Error(w, "missing job id", http.StatusBadRequest)
				return
			}
			jobID := parts[0]
			if len(parts) == 2 && parts[1] == "history" {
				if r.Method == http.MethodGet {
					getJobHistoryHTTP(w, r, sched, jobID)
					return
				}
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if r.Method == http.MethodDelete {
				deleteJobHTTP(w, r, sched, jobID)
				return
			}
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		})

		mux.HandleFunc("/api/targets", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				listTargetsHTTP(w, r, sched)
			case http.MethodPost:
				addTargetHTTP(w, r, sched)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
		})
		mux.HandleFunc("/api/targets/", func(w http.ResponseWriter, r *http.Request) {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/targets/"), "/")
			if len(parts) == 0 || parts[0] == "" {
				http.Error(w, "missing target id", http.StatusBadRequest)
				return
			}
			targetID := parts[0]
			if r.Method == http.MethodDelete {
				deleteTargetHTTP(w, r, sched, targetID)
				return
			}
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		})

		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" || r.URL.Path == "/index.html" {
				w.Header().Set("Content-Type", "text/html")
				w.Write(webIndex)
				return
			}
			http.NotFound(w, r)
		})

		log.Printf("Web interface started on http://0.0.0.0:%s", *httpPort)
		if err := http.ListenAndServe(":"+*httpPort, mux); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Блокируем main, пока работают серверы
	select {}
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

// HTTP-обработчики
func listJobsHTTP(w http.ResponseWriter, r *http.Request, sched *scheduler.Scheduler) {
	jobs := sched.ListJobs()
	writeJSON(w, octocron.ListJobsResponse{Jobs: jobs})
}

func createJobHTTP(w http.ResponseWriter, r *http.Request, sched *scheduler.Scheduler) {
	var req octocron.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	job := &octocron.Job{
		Id:             uuid.New().String(),
		Name:           req.Name,
		CronExpression: req.CronExpression,
		Command:        req.Command,
		Targets:        req.Targets,
		Enabled:        true,
		CreatedAt:      timestamppb.New(time.Now()),
	}
	if err := sched.AddJob(job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, job)
}

func deleteJobHTTP(w http.ResponseWriter, r *http.Request, sched *scheduler.Scheduler, jobID string) {
	if err := sched.RemoveJob(jobID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"result":"ok"}`))
}

func getJobHistoryHTTP(w http.ResponseWriter, r *http.Request, sched *scheduler.Scheduler, jobID string) {
	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	history := sched.GetJobHistory(jobID, limit)
	writeJSON(w, octocron.ListJobHistoryResponse{History: history})
}

func listTargetsHTTP(w http.ResponseWriter, r *http.Request, sched *scheduler.Scheduler) {
	targets := sched.ListTargets()
	writeJSON(w, octocron.ListTargetsResponse{Targets: targets})
}

func addTargetHTTP(w http.ResponseWriter, r *http.Request, sched *scheduler.Scheduler) {
	var req octocron.AddTargetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	target := &octocron.Target{
		Id:      uuid.New().String(),
		Name:    req.Name,
		Address: req.Address,
		Port:    req.Port,
		User:    req.User,
		KeyPath: req.KeyPath,
	}
	if err := sched.AddTarget(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, target)
}

func deleteTargetHTTP(w http.ResponseWriter, r *http.Request, sched *scheduler.Scheduler, targetID string) {
	if err := sched.RemoveTarget(targetID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"result":"ok"}`))
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
