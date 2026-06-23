package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	grpcapi "github.com/NillHellberg/octocron/internal/api/grpc"
	"github.com/NillHellberg/octocron/internal/scheduler"
)

func main() {
	// Создаём планировщик с локальным исполнителем
	sched := scheduler.NewScheduler(nil)
	sched.Start()
	defer sched.Stop()

	// gRPC сервер
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	octocron.RegisterOctocronServer(s, grpcapi.NewOctocronServer(sched))
	reflection.Register(s) // для grpcurl

	log.Println("Octocron server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
