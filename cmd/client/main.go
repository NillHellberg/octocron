package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/NillHellberg/octocron/api/gen/octocron"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := octocron.NewOctocronClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Создать задание
	job, err := c.CreateJob(ctx, &octocron.CreateJobRequest{
		Name:           "test-job",
		CronExpression: "*/10 * * * * *", // каждые 10 секунд (секунды включены)
		Command:        "echo Hello from Octocron",
	})
	if err != nil {
		log.Fatalf("create failed: %v", err)
	}
	log.Printf("Created job: %+v", job)

	// Получить список
	resp, err := c.ListJobs(ctx, &octocron.ListJobsRequest{})
	if err != nil {
		log.Fatalf("list failed: %v", err)
	}
	log.Printf("Jobs: %+v", resp.Jobs)
}
