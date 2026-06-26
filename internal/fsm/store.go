package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CommandType string

const (
	CmdCreateJob         CommandType = "CREATE_JOB"
	CmdDeleteJob         CommandType = "DELETE_JOB"
	CmdUpdateJobLastRun  CommandType = "UPDATE_JOB_LAST_RUN"
	CmdAddTarget         CommandType = "ADD_TARGET"
	CmdRemoveTarget      CommandType = "REMOVE_TARGET"
	CmdAddJobExecution   CommandType = "ADD_JOB_EXECUTION"
)

type Command struct {
	Type      CommandType
	Job       *octocron.Job
	LastRun   *timestamppb.Timestamp
	Target    *octocron.Target
	Execution *octocron.JobExecution
}

const maxHistoryPerJob = 100

type JobStore struct {
	mu       sync.RWMutex
	jobs     map[string]*octocron.Job
	targets  map[string]*octocron.Target
	history  map[string][]*octocron.JobExecution // ключ – job_id
}

func NewJobStore() *JobStore {
	return &JobStore{
		jobs:    make(map[string]*octocron.Job),
		targets: make(map[string]*octocron.Target),
		history: make(map[string][]*octocron.JobExecution),
	}
}

func (s *JobStore) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch cmd.Type {
	case CmdCreateJob:
		s.mu.Lock()
		s.jobs[cmd.Job.Id] = cmd.Job
		s.mu.Unlock()
	case CmdDeleteJob:
		s.mu.Lock()
		delete(s.jobs, cmd.Job.Id)
		// историю удалять не будем, чтобы осталась доступной
		s.mu.Unlock()
	case CmdUpdateJobLastRun:
		s.mu.Lock()
		if job, ok := s.jobs[cmd.Job.Id]; ok {
			job.LastRun = cmd.LastRun
		}
		s.mu.Unlock()
	case CmdAddTarget:
		s.mu.Lock()
		s.targets[cmd.Target.Id] = cmd.Target
		s.mu.Unlock()
	case CmdRemoveTarget:
		s.mu.Lock()
		delete(s.targets, cmd.Target.Id)
		s.mu.Unlock()
	case CmdAddJobExecution:
		s.mu.Lock()
		jobID := cmd.Execution.JobId
		s.history[jobID] = append(s.history[jobID], cmd.Execution)
		if len(s.history[jobID]) > maxHistoryPerJob {
			// Ограничиваем размер, удаляя старые записи
			excess := len(s.history[jobID]) - maxHistoryPerJob
			s.history[jobID] = s.history[jobID][excess:]
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *JobStore) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapData := struct {
		Jobs    map[string]*octocron.Job
		Targets map[string]*octocron.Target
		History map[string][]*octocron.JobExecution
	}{
		Jobs:    copyJobsMap(s.jobs),
		Targets: copyTargetsMap(s.targets),
		History: copyHistoryMap(s.history),
	}
	return &JobSnapshot{data: snapData}, nil
}

func (s *JobStore) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	s.mu.Lock()
	defer s.mu.Unlock()

	var snapData struct {
		Jobs    map[string]*octocron.Job
		Targets map[string]*octocron.Target
		History map[string][]*octocron.JobExecution
	}
	decoder := json.NewDecoder(rc)
	if err := decoder.Decode(&snapData); err != nil {
		return err
	}
	s.jobs = snapData.Jobs
	s.targets = snapData.Targets
	s.history = snapData.History
	return nil
}

// Вспомогательные функции копирования мап (для снапшотов)
func copyJobsMap(src map[string]*octocron.Job) map[string]*octocron.Job {
	dst := make(map[string]*octocron.Job, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
func copyTargetsMap(src map[string]*octocron.Target) map[string]*octocron.Target {
	dst := make(map[string]*octocron.Target, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
func copyHistoryMap(src map[string][]*octocron.JobExecution) map[string][]*octocron.JobExecution {
	dst := make(map[string][]*octocron.JobExecution, len(src))
	for k, v := range src {
		dst[k] = append([]*octocron.JobExecution{}, v...)
	}
	return dst
}

// Интерфейс снапшота
type JobSnapshot struct {
	data interface{}
}

func (js *JobSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(js.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (js *JobSnapshot) Release() {}

// Методы доступа к данным (используются планировщиком и gRPC)
func (s *JobStore) GetJob(id string) (*octocron.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job not found")
	}
	return job, nil
}

func (s *JobStore) ListJobs() []*octocron.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*octocron.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (s *JobStore) GetTarget(id string) (*octocron.Target, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.targets[id]
	if !ok {
		return nil, fmt.Errorf("target not found")
	}
	return t, nil
}

func (s *JobStore) ListTargets() []*octocron.Target {
	s.mu.RLock()
	defer s.mu.RUnlock()
	targets := make([]*octocron.Target, 0, len(s.targets))
	for _, t := range s.targets {
		targets = append(targets, t)
	}
	return targets
}

func (s *JobStore) GetJobHistory(jobID string, limit int) []*octocron.JobExecution {
	s.mu.RLock()
	defer s.mu.RUnlock()
	history, ok := s.history[jobID]
	if !ok {
		return nil
	}
	if limit <= 0 || limit > len(history) {
		limit = len(history)
	}
	// возвращаем последние limit записей
	start := len(history) - limit
	return history[start:]
}
