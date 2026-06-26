package main

import (
	"context"
	"fmt"	
	"os"
	"time"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var serverAddr string

func main() {
	rootCmd := &cobra.Command{
		Use:   "octoctl",
		Short: "Octocron CLI",
		Long:  "Command line interface for Octocron distributed cron scheduler.",
	}

	rootCmd.PersistentFlags().StringVar(&serverAddr, "server", "localhost:50051", "gRPC server address")

	// Команды для заданий
	jobCmd := &cobra.Command{Use: "job", Short: "Manage jobs"}
	jobCmd.AddCommand(
		&cobra.Command{
			Use:   "create",
			Short: "Create a new job",
			RunE: func(cmd *cobra.Command, args []string) error {
				name, _ := cmd.Flags().GetString("name")
				cron, _ := cmd.Flags().GetString("cron")
				command, _ := cmd.Flags().GetString("command")
				targets, _ := cmd.Flags().GetStringSlice("target")
				return createJob(name, cron, command, targets)
			},
		},
		&cobra.Command{
			Use:   "list",
			Short: "List all jobs",
			RunE: func(cmd *cobra.Command, args []string) error {
				return listJobs()
			},
		},
		&cobra.Command{
			Use:   "delete [id]",
			Short: "Delete a job",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return deleteJob(args[0])
			},
		},
		&cobra.Command{
			Use:   "history [id]",
			Short: "Show job execution history",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				limit, _ := cmd.Flags().GetInt("limit")
				return getJobHistory(args[0], limit)
			},
		},
	)

	createFlags := jobCmd.Commands()[0].Flags()
	createFlags.String("name", "", "Job name")
	createFlags.String("cron", "", "Cron expression (e.g. '*/10 * * * * *')")
	createFlags.String("command", "", "Command to execute")
	createFlags.StringSlice("target", nil, "Target IDs (can be repeated)")
	jobCmd.Commands()[0].MarkFlagRequired("name")
	jobCmd.Commands()[0].MarkFlagRequired("cron")
	jobCmd.Commands()[0].MarkFlagRequired("command")

	historyCmd := jobCmd.Commands()[3]
	historyCmd.Flags().Int("limit", 10, "Number of last executions to show")

	// Команды для целевых хостов
	targetCmd := &cobra.Command{Use: "target", Short: "Manage target hosts"}
	targetCmd.AddCommand(
		&cobra.Command{
			Use:   "add",
			Short: "Add a new target host",
			RunE: func(cmd *cobra.Command, args []string) error {
				name, _ := cmd.Flags().GetString("name")
				address, _ := cmd.Flags().GetString("address")
				port, _ := cmd.Flags().GetInt("port")
				user, _ := cmd.Flags().GetString("user")
				keyPath, _ := cmd.Flags().GetString("key")
				return addTarget(name, address, port, user, keyPath)
			},
		},
		&cobra.Command{
			Use:   "list",
			Short: "List all targets",
			RunE: func(cmd *cobra.Command, args []string) error {
				return listTargets()
			},
		},
		&cobra.Command{
			Use:   "remove [id]",
			Short: "Remove a target",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return removeTarget(args[0])
			},
		},
	)

	targetAddFlags := targetCmd.Commands()[0].Flags()
	targetAddFlags.String("name", "", "Target name")
	targetAddFlags.String("address", "", "IP or hostname")
	targetAddFlags.Int("port", 22, "SSH port")
	targetAddFlags.String("user", "", "SSH user")
	targetAddFlags.String("key", "", "Path to private key")
	targetCmd.Commands()[0].MarkFlagRequired("name")
	targetCmd.Commands()[0].MarkFlagRequired("address")
	targetCmd.Commands()[0].MarkFlagRequired("user")
	targetCmd.Commands()[0].MarkFlagRequired("key")

	rootCmd.AddCommand(jobCmd, targetCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getClient() (octocron.OctocronClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("could not connect: %v", err)
	}
	return octocron.NewOctocronClient(conn), conn, nil
}

func createJob(name, cronExpr, command string, targets []string) error {
	client, conn, err := getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CreateJob(ctx, &octocron.CreateJobRequest{
		Name:           name,
		CronExpression: cronExpr,
		Command:        command,
		Targets:        targets,
	})
	if err != nil {
		return fmt.Errorf("create job failed: %v", err)
	}
	fmt.Printf("Job created: %s\n", resp.Id)
	return nil
}

func listJobs() error {
	client, conn, err := getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ListJobs(ctx, &octocron.ListJobsRequest{})
	if err != nil {
		return fmt.Errorf("list jobs failed: %v", err)
	}
	if len(resp.Jobs) == 0 {
		fmt.Println("No jobs found.")
		return nil
	}
	for _, job := range resp.Jobs {
		fmt.Printf("ID: %s\n  Name: %s\n  Cron: %s\n  Command: %s\n  Targets: %v\n  Enabled: %v\n\n",
			job.Id, job.Name, job.CronExpression, job.Command, job.Targets, job.Enabled)
	}
	return nil
}

func deleteJob(id string) error {
	client, conn, err := getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.DeleteJob(ctx, &octocron.DeleteJobRequest{Id: id})
	if err != nil {
		return fmt.Errorf("delete job failed: %v", err)
	}
	fmt.Printf("Job %s deleted.\n", id)
	return nil
}

func getJobHistory(id string, limit int) error {
	client, conn, err := getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetJobHistory(ctx, &octocron.GetJobHistoryRequest{
		JobId: id,
		Limit: int32(limit),
	})
	if err != nil {
		return fmt.Errorf("get history failed: %v", err)
	}
	if len(resp.History) == 0 {
		fmt.Println("No execution history.")
		return nil
	}
	for _, exec := range resp.History {
		fmt.Printf("Time: %s\n  Target: %s\n  ExitCode: %d\n  Output: %s\n  Error: %s\n\n",
			exec.StartTime.AsTime().Format(time.RFC3339),
			exec.TargetId,
			exec.ExitCode,
			exec.Output,
			exec.Error,
		)
	}
	return nil
}

func addTarget(name, address string, port int, user, keyPath string) error {
	client, conn, err := getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.AddTarget(ctx, &octocron.AddTargetRequest{
		Name:    name,
		Address: address,
		Port:    int32(port),
		User:    user,
		KeyPath: keyPath,
	})
	if err != nil {
		return fmt.Errorf("add target failed: %v", err)
	}
	fmt.Printf("Target added: %s\n", resp.Id)
	return nil
}

func listTargets() error {
	client, conn, err := getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ListTargets(ctx, &octocron.ListTargetsRequest{})
	if err != nil {
		return fmt.Errorf("list targets failed: %v", err)
	}
	if len(resp.Targets) == 0 {
		fmt.Println("No targets found.")
		return nil
	}
	for _, t := range resp.Targets {
		fmt.Printf("ID: %s\n  Name: %s\n  Address: %s:%d\n  User: %s\n  Key: %s\n\n",
			t.Id, t.Name, t.Address, t.Port, t.User, t.KeyPath)
	}
	return nil
}

func removeTarget(id string) error {
	client, conn, err := getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.RemoveTarget(ctx, &octocron.RemoveTargetRequest{Id: id})
	if err != nil {
		return fmt.Errorf("remove target failed: %v", err)
	}
	fmt.Printf("Target %s removed.\n", id)
	return nil
}
