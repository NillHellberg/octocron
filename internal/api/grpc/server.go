package grpc

import (
	"context"
	"time"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	"github.com/NillHellberg/octocron/internal/scheduler"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OctocronServer struct {
	octocron.UnimplementedOctocronServer
	sched *scheduler.Scheduler
}

func NewOctocronServer(s *scheduler.Scheduler) *OctocronServer {
	return &OctocronServer{sched: s}
}

func (s *OctocronServer) CreateJob(ctx context.Context, req *octocron.CreateJobRequest) (*octocron.Job, error) {
	job := &octocron.Job{
		Id:             uuid.New().String(),
		Name:           req.Name,
		CronExpression: req.CronExpression,
		Command:        req.Command,
		Targets:        req.Targets,
		Enabled:        true, // по умолчанию активно
		CreatedAt:      timestamppb.New(time.Now()),
	}
	if err := s.sched.AddJob(job); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to add job: %v", err)
	}
	return job, nil
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

func (s *OctocronServer) DeleteJob(ctx context.Context, req *octocron.DeleteJobRequest) (*emptypb.Empty, error) {
	if err := s.sched.RemoveJob(req.Id); err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to delete: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *OctocronServer) GetJobHistory(ctx context.Context, req *octocron.GetJobHistoryRequest) (*octocron.ListJobHistoryResponse, error) {
	// Пока возвращаем пустую историю
	return &octocron.ListJobHistoryResponse{}, nil
}
