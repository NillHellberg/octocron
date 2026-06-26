package scheduler

import (
	"fmt"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	"github.com/NillHellberg/octocron/internal/executor"
	"github.com/NillHellberg/octocron/internal/fsm"
	"github.com/hashicorp/raft"
	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Scheduler struct {
	exec         executor.SSHExecutor
	raft         *raft.Raft
	store        *fsm.JobStore

	stopCh       chan struct{}
	leaderCh     chan bool
	isLeader     bool
	mu           sync.Mutex

	lastLocalRun map[string]time.Time
	localRunMu   sync.Mutex
}

func NewScheduler(r *raft.Raft, store *fsm.JobStore) *Scheduler {
	s := &Scheduler{
		exec:         *executor.NewSSHExecutor(),
		raft:         r,
		store:        store,
		stopCh:       make(chan struct{}),
		leaderCh:     make(chan bool, 1),
		lastLocalRun: make(map[string]time.Time),
	}
	go s.observeLeadership()
	go s.run()
	return s
}

func (s *Scheduler) observeLeadership() {
	for range s.raft.LeaderCh() {
		isLeader := s.raft.State() == raft.Leader
		s.mu.Lock()
		s.isLeader = isLeader
		s.mu.Unlock()
		select {
		case s.leaderCh <- isLeader:
		default:
		}
		if isLeader {
			log.Println("I am leader, starting stateless cron")
		} else {
			log.Println("Not leader, stopping stateless cron")
			s.localRunMu.Lock()
			s.lastLocalRun = make(map[string]time.Time)
			s.localRunMu.Unlock()
		}
	}
}

func (s *Scheduler) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			leader := s.isLeader
			s.mu.Unlock()
			if !leader {
				continue
			}
			s.checkAndExecute()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Scheduler) checkAndExecute() {
	jobs := s.store.ListJobs()
	now := time.Now()

	for _, job := range jobs {
		if !job.Enabled {
			continue
		}
		shouldRun, err := s.shouldRun(job, now)
		if err != nil {
			log.Printf("Error parsing cron expression for job %s: %v", job.Id, err)
			continue
		}
		if !shouldRun {
			continue
		}

		// Выполняем для каждого target
		for _, targetID := range job.Targets {
			target, err := s.store.GetTarget(targetID)
			if err != nil {
				log.Printf("Job %s: target %s not found: %v", job.Id, targetID, err)
				continue
			}

			startTime := time.Now()
			log.Printf("Executing job %s on %s (%s): %s", job.Id, target.Name, target.Address, job.Command)
			exitCode, stdout, stderr, err := s.exec.Execute(
				target.Address,
				int(target.Port),
				target.User,
				target.KeyPath,
				job.Command,
			)
			endTime := time.Now()
			log.Printf("Job %s on %s finished: code=%d", job.Id, target.Name, exitCode)

			// Сохраняем результат выполнения в FSM
			execution := &octocron.JobExecution{
				JobId:     job.Id,
				TargetId:  targetID,
				StartTime: timestamppb.New(startTime),
				EndTime:   timestamppb.New(endTime),
				ExitCode:  int32(exitCode),
				Output:    stdout,
				Error:     stderr,
			}
			if err != nil {
				// SSH-ошибка тоже сохраняется
				execution.Error = fmt.Sprintf("ssh error: %v", err)
				execution.ExitCode = -1
			}
			cmd := fsm.Command{
				Type:      fsm.CmdAddJobExecution,
				Execution: execution,
			}
			data, err := json.Marshal(cmd)
			if err != nil {
				log.Printf("Failed to marshal execution for job %s: %v", job.Id, err)
				continue
			}
			f := s.raft.Apply(data, 5*time.Second)
			if f.Error() != nil {
				log.Printf("Failed to save execution for job %s: %v", job.Id, f.Error())
			}
		}

		// Обновляем last_run в FSM
		newLastRun := timestamppb.New(now)
		cmd := fsm.Command{
			Type:    fsm.CmdUpdateJobLastRun,
			Job:     &octocron.Job{Id: job.Id},
			LastRun: newLastRun,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			log.Printf("Failed to marshal update command for job %s: %v", job.Id, err)
			continue
		}
		f := s.raft.Apply(data, 5*time.Second)
		if f.Error() != nil {
			log.Printf("Failed to apply update for job %s: %v", job.Id, f.Error())
		}

		s.localRunMu.Lock()
		s.lastLocalRun[job.Id] = now
		s.localRunMu.Unlock()
	}
}

func (s *Scheduler) shouldRun(job *octocron.Job, now time.Time) (bool, error) {
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(job.CronExpression)
	if err != nil {
		return false, err
	}

	var lastTime time.Time
	if job.LastRun != nil {
		lastTime = job.LastRun.AsTime()
	}

	s.localRunMu.Lock()
	if localRun, ok := s.lastLocalRun[job.Id]; ok && localRun.After(lastTime) {
		lastTime = localRun
	}
	s.localRunMu.Unlock()

	next := schedule.Next(lastTime)
	return !next.After(now), nil
}

func (s *Scheduler) AddJob(job *octocron.Job) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	cmd := fsm.Command{
		Type: fsm.CmdCreateJob,
		Job:  job,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(data, 10*time.Second)
	return f.Error()
}

func (s *Scheduler) RemoveJob(id string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	cmd := fsm.Command{
		Type: fsm.CmdDeleteJob,
		Job:  &octocron.Job{Id: id},
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(data, 10*time.Second)
	return f.Error()
}

func (s *Scheduler) GetJob(id string) (*octocron.Job, error) {
	return s.store.GetJob(id)
}

func (s *Scheduler) ListJobs() []*octocron.Job {
	return s.store.ListJobs()
}

// Методы для целевых хостов
func (s *Scheduler) AddTarget(target *octocron.Target) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	cmd := fsm.Command{
		Type:   fsm.CmdAddTarget,
		Target: target,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(data, 10*time.Second)
	return f.Error()
}

func (s *Scheduler) RemoveTarget(id string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	cmd := fsm.Command{
		Type:   fsm.CmdRemoveTarget,
		Target: &octocron.Target{Id: id},
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(data, 10*time.Second)
	return f.Error()
}

func (s *Scheduler) GetTarget(id string) (*octocron.Target, error) {
	return s.store.GetTarget(id)
}

func (s *Scheduler) ListTargets() []*octocron.Target {
	return s.store.ListTargets()
}

func (s *Scheduler) GetJobHistory(jobID string, limit int) []*octocron.JobExecution {
	return s.store.GetJobHistory(jobID, limit)
}

var (
	ErrNotLeader   = &SchedulerError{"not leader"}
	ErrJobExists   = &SchedulerError{"job already exists"}
	ErrJobNotFound = &SchedulerError{"job not found"}
)

type SchedulerError struct {
	msg string
}

func (e *SchedulerError) Error() string { return e.msg }
