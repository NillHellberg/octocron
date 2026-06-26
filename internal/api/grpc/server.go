package grpc

import (
	"context"
	"time"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	"github.com/NillHellberg/octocron/internal/scheduler"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OctocronServer struct {
	octocron.UnimplementedOctocronServer
	sched *scheduler.Scheduler
	raft  *raft.Raft
}

func NewOctocronServer(s *scheduler.Scheduler, r *raft.Raft) *OctocronServer {
	return &OctocronServer{sched: s, raft: r}
}

func (s *OctocronServer) requireLeader() error {
	if s.raft.State() != raft.Leader {
		leaderAddr := s.raft.Leader()
		if leaderAddr == "" {
			return status.Errorf(codes.Unavailable, "no leader elected")
		}
		return status.Errorf(codes.FailedPrecondition, "not leader, leader is at %s", leaderAddr)
	}
	return nil
}

func (s *OctocronServer) CreateJob(ctx context.Context, req *octocron.CreateJobRequest) (*octocron.Job, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
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
	if err := s.sched.AddJob(job); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add job: %v", err)
	}
	return job, nil
}

func (s *OctocronServer) DeleteJob(ctx context.Context, req *octocron.DeleteJobRequest) (*emptypb.Empty, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if err := s.sched.RemoveJob(req.Id); err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to delete job: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *OctocronServer) GetJob(ctx context.Context, req *octocron.GetJobRequest) (*octocron.Job, error) {
	job, err := s.sched.GetJob(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job not found: %v", err)
	}
	return job, nil
}

func (s *OctocronServer) ListJobs(ctx context.Context, req *octocron.ListJobsRequest) (*octocron.ListJobsResponse, error) {
	jobs := s.sched.ListJobs()
	return &octocron.ListJobsResponse{Jobs: jobs}, nil
}

func (s *OctocronServer) AddTarget(ctx context.Context, req *octocron.AddTargetRequest) (*octocron.Target, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	target := &octocron.Target{
		Id:      uuid.New().String(),
		Name:    req.Name,
		Address: req.Address,
		Port:    req.Port,
		User:    req.User,
		KeyPath: req.KeyPath,
	}
	if err := s.sched.AddTarget(target); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add target: %v", err)
	}
	return target, nil
}

func (s *OctocronServer) RemoveTarget(ctx context.Context, req *octocron.RemoveTargetRequest) (*emptypb.Empty, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if err := s.sched.RemoveTarget(req.Id); err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to remove target: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *OctocronServer) ListTargets(ctx context.Context, req *octocron.ListTargetsRequest) (*octocron.ListTargetsResponse, error) {
	targets := s.sched.ListTargets()
	return &octocron.ListTargetsResponse{Targets: targets}, nil
}

func (s *OctocronServer) GetJobHistory(ctx context.Context, req *octocron.GetJobHistoryRequest) (*octocron.ListJobHistoryResponse, error) {
	history := s.sched.GetJobHistory(req.JobId, int(req.Limit))
	return &octocron.ListJobHistoryResponse{History: history}, nil
}

func (s *OctocronServer) Join(ctx context.Context, req *octocron.JoinRequest) (*emptypb.Empty, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	cfgFuture := s.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get configuration: %v", err)
	}
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(req.NodeId) {
			return &emptypb.Empty{}, nil
		}
	}
	f := s.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddress), 0, 0)
	if err := f.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add voter: %v", err)
	}
	return &emptypb.Empty{}, nil
}
