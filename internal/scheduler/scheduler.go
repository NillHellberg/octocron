package scheduler

import (
	"sync"

	"github.com/NillHellberg/octocron/api/gen/octocron"
	"github.com/robfig/cron/v3"
)

// JobRecord хранит задание и его cron-сущность.
type JobRecord struct {
	Job      *octocron.Job
	cronID   cron.EntryID
	enabled  bool
}

// Scheduler управляет заданиями.
type Scheduler struct {
	mu    sync.RWMutex
	jobs  map[string]*JobRecord
	cr    *cron.Cron
	exec  Executor
}

// Executor выполняет команды (пока локально).
type Executor interface {
	Execute(cmd string) (exitCode int, output, errStr string)
}

// LocalExecutor реализует запуск команды локально.
type LocalExecutor struct{}

func (e LocalExecutor) Execute(cmd string) (int, string, string) {
	// Заглушка — будем использовать os/exec позже
	return 0, "local executed: " + cmd, ""
}

func NewScheduler(exec Executor) *Scheduler {
	if exec == nil {
		exec = &LocalExecutor{}
	}
	return &Scheduler{
		jobs: make(map[string]*JobRecord),
		cr:   cron.New(cron.WithSeconds()),
		exec: exec,
	}
}

// Start запускает cron-движок.
func (s *Scheduler) Start() {
	s.cr.Start()
}

// Stop останавливает cron-движок.
func (s *Scheduler) Stop() {
	ctx := s.cr.Stop()
	<-ctx.Done()
}

// AddJob добавляет задание и запускает его по cron.
func (s *Scheduler) AddJob(job *octocron.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.Id]; exists {
		return ErrJobExists
	}

	rec := &JobRecord{
		Job:     job,
		enabled: job.Enabled,
	}

	if job.Enabled {
		cronID, err := s.cr.AddFunc(job.CronExpression, func() {
			exitCode, out, errStr := s.exec.Execute(job.Command)
			_ = exitCode // TODO: сохранять историю выполнения
			_ = out
			_ = errStr
		})
		if err != nil {
			return err
		}
		rec.cronID = cronID
	}

	s.jobs[job.Id] = rec
	return nil
}

// UpdateJob обновляет задание (пока заглушка).
func (s *Scheduler) UpdateJob(job *octocron.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.jobs[job.Id]
	if !exists {
		return ErrJobNotFound
	}

	// Удаляем старую cron-запись, если была активна
	if rec.enabled {
		s.cr.Remove(rec.cronID)
	}

	rec.Job = job
	rec.enabled = job.Enabled
	if job.Enabled {
		cronID, err := s.cr.AddFunc(job.CronExpression, func() {
			// выполнение
		})
		if err != nil {
			return err
		}
		rec.cronID = cronID
	} else {
		rec.cronID = 0
	}
	return nil
}

// RemoveJob удаляет задание.
func (s *Scheduler) RemoveJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.jobs[id]
	if !exists {
		return ErrJobNotFound
	}
	if rec.enabled {
		s.cr.Remove(rec.cronID)
	}
	delete(s.jobs, id)
	return nil
}

// GetJob возвращает задание по ID.
func (s *Scheduler) GetJob(id string) (*octocron.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return rec.Job, nil
}

// ListJobs возвращает все задания.
func (s *Scheduler) ListJobs() []*octocron.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*octocron.Job, 0, len(s.jobs))
	for _, rec := range s.jobs {
		jobs = append(jobs, rec.Job)
	}
	return jobs
}

// Ошибки
var (
	ErrJobExists   = &SchedulerError{"job already exists"}
	ErrJobNotFound = &SchedulerError{"job not found"}
)

type SchedulerError struct {
	msg string
}

func (e *SchedulerError) Error() string { return e.msg }
