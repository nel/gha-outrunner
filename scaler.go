package outrunner

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/google/uuid"
)

// ScaleSetClient is the subset of scaleset.Client that Scaler uses.
// Extracted as an interface for testability.
type ScaleSetClient interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

// Scaler implements listener.Scaler by provisioning real runner environments.
// Each runner gets its own goroutine that manages the full lifecycle:
// provisioning, waiting for job completion, stopping, and deregistration.
type Scaler struct {
	logger      *slog.Logger
	client      ScaleSetClient
	scaleSetID  int
	maxRunners  int
	namePrefix  string
	runner      *RunnerConfig
	provisioner Provisioner
	// budget, shared by every scale set, admits each runner before it is
	// provisioned; nil admits all.
	budget *MemoryBudget

	mu      sync.Mutex
	runners map[string]*RunnerState
	// assignedJobs is the latest TotalAssignedJobs from the listener, and
	// completedSinceCount the jobs completed since it arrived. Refill after
	// cleanup uses their difference.
	assignedJobs        int
	completedSinceCount int

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	wg              sync.WaitGroup
}

var _ listener.Scaler = (*Scaler)(nil)

func NewScaler(logger *slog.Logger, client ScaleSetClient, scaleSetID, maxRunners int, namePrefix string, runner *RunnerConfig, prov Provisioner) *Scaler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scaler{
		logger:          logger,
		client:          client,
		scaleSetID:      scaleSetID,
		maxRunners:      maxRunners,
		namePrefix:      namePrefix,
		runner:          runner,
		provisioner:     prov,
		runners:         make(map[string]*RunnerState),
		lifecycleCtx:    ctx,
		lifecycleCancel: cancel,
	}
}

func (s *Scaler) HandleDesiredRunnerCount(ctx context.Context, count int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.assignedJobs = count
	s.completedSinceCount = 0
	return s.scaleLocked(ctx, count)
}

// scaleLocked spawns runners until count assigned jobs are covered, capped at
// maxRunners. Callers hold s.mu.
func (s *Scaler) scaleLocked(ctx context.Context, count int) (int, error) {
	target := min(s.maxRunners, count)
	current := len(s.runners)

	s.logger.Debug("Desired runner count",
		slog.Int("requested", count),
		slog.Int("target", target),
		slog.Int("current", current),
	)

	for range target - current {
		name := fmt.Sprintf("%s-%s", s.namePrefix, uuid.NewString()[:8])

		jit, err := s.client.GenerateJitRunnerConfig(ctx,
			&scaleset.RunnerScaleSetJitRunnerSetting{Name: name},
			s.scaleSetID,
		)
		if err != nil {
			return len(s.runners), fmt.Errorf("generate JIT config: %w", err)
		}

		state := &RunnerState{
			Name:      name,
			RunnerID:  jit.Runner.ID,
			Phase:     RunnerProvisioning,
			CreatedAt: time.Now(),
			done:      make(chan struct{}),
		}
		s.runners[name] = state

		req := &RunnerRequest{
			Name:      name,
			JITConfig: jit.EncodedJITConfig,
			Runner:    s.runner,
		}

		s.logger.Info("Spawning runner",
			slog.String("name", name),
			slog.Int("runnerID", state.RunnerID),
		)

		s.wg.Add(1)
		go s.runRunnerLifecycle(state, req)
	}

	return len(s.runners), nil
}

func (s *Scaler) HandleJobStarted(ctx context.Context, jobInfo *scaleset.JobStarted) error {
	s.mu.Lock()
	state, exists := s.runners[jobInfo.RunnerName]
	if exists {
		state.Phase = RunnerRunning
		state.job = jobInfo.JobDisplayName
		// owner/repo/.github/workflows/file.yml@ref
		state.workflow, _, _ = strings.Cut(jobInfo.JobWorkflowRef, "@")
	}
	s.mu.Unlock()

	s.logger.Info("Job started",
		slog.String("runnerName", jobInfo.RunnerName),
		slog.Int64("requestId", jobInfo.RunnerRequestID),
	)
	return nil
}

func (s *Scaler) HandleJobCompleted(ctx context.Context, jobInfo *scaleset.JobCompleted) error {
	s.logger.Info("Job completed",
		slog.String("runnerName", jobInfo.RunnerName),
		slog.String("result", jobInfo.Result),
	)

	s.mu.Lock()
	state, exists := s.runners[jobInfo.RunnerName]
	if exists {
		state.servedJob = true
		s.completedSinceCount++
	}
	s.mu.Unlock()

	if exists {
		state.SignalDone()
	}

	return nil
}

// Shutdown cancels all runner goroutines and waits for them to finish.
func (s *Scaler) Shutdown(ctx context.Context) {
	s.lifecycleCancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("All runners shut down")
	case <-ctx.Done():
		s.logger.Warn("Shutdown timed out, some runners may not be fully cleaned up")
	}
}

// RunnerSnapshot is a point-in-time copy of a runner's state, safe to read
// without holding the scaler's mutex.
type RunnerSnapshot struct {
	Name      string
	RunnerID  int
	Phase     RunnerPhase
	CreatedAt time.Time
	StartedAt time.Time
}

// Runners returns a snapshot of current runner states.
func (s *Scaler) Runners() []RunnerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshots := make([]RunnerSnapshot, 0, len(s.runners))
	for _, state := range s.runners {
		snapshots = append(snapshots, RunnerSnapshot{
			Name:      state.Name,
			RunnerID:  state.RunnerID,
			Phase:     state.Phase,
			CreatedAt: state.CreatedAt,
			StartedAt: state.StartedAt,
		})
	}
	return snapshots
}

// runRunnerLifecycle manages the full lifecycle of a single runner.
// Runs in its own goroutine. Handles provisioning, waiting, stopping,
// deregistration, and map cleanup.
func (s *Scaler) runRunnerLifecycle(state *RunnerState, req *RunnerRequest) {
	defer s.wg.Done()
	defer s.removeRunner(state)

	name := state.Name

	// 1. Wait for host memory, held until the environment is stopped.
	reservation := req.Runner.ReservationMB()
	sampler, _ := s.provisioner.(MemorySampler)
	waited := false
	grant, err := s.budget.Acquire(s.lifecycleCtx, name, reservation, func() {
		waited = true
		s.logger.Info("Waiting for host memory", slog.String("name", name), slog.Int("memoryMB", reservation))
	})
	if err != nil {
		s.deregisterRunner(name, state.RunnerID)
		return
	}
	if waited {
		s.logger.Info("Host memory admitted", slog.String("name", name),
			slog.Duration("waited", time.Since(state.CreatedAt).Round(time.Second)))
	}
	defer grant.Release()

	// 2. Provision
	if err := s.provisioner.Start(s.lifecycleCtx, req); err != nil {
		s.logger.Error("Failed to start runner",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		s.deregisterRunner(name, state.RunnerID)
		return
	}

	s.mu.Lock()
	if state.Phase == RunnerRunning {
		// HandleJobStarted arrived during provisioning
		s.logger.Info("Runner provisioned (job already assigned)", slog.String("name", name))
	} else {
		state.Phase = RunnerIdle
		s.logger.Info("Runner provisioned", slog.String("name", name))
	}
	state.StartedAt = time.Now()
	s.mu.Unlock()

	// 3. Wait for completion signal or shutdown, measuring the job's memory.
	stopSampling := s.sampleMemory(sampler, name, grant)
	select {
	case <-state.done:
		s.logger.Debug("Runner signaled done", slog.String("name", name))
	case <-s.lifecycleCtx.Done():
		s.logger.Debug("Runner shutdown requested", slog.String("name", name))
	}
	if peak, measured := stopSampling(); measured {
		s.mu.Lock()
		job, workflow := state.job, state.workflow
		s.mu.Unlock()
		s.logMemoryPeak(name, job, workflow, reservation, peak)
	}

	// 4. Stop
	s.mu.Lock()
	state.Phase = RunnerStopping
	s.mu.Unlock()

	s.logger.Info("Stopping runner", slog.String("name", name))
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stopCancel()
	if err := s.provisioner.Stop(stopCtx, name); err != nil {
		s.logger.Error("Failed to stop runner",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
	}

	// 5. Deregister from GitHub
	s.deregisterRunner(name, state.RunnerID)
}

// memorySampleInterval is how often a running job's memory is measured: often
// enough to see short peaks of a job's compilers and service containers.
const memorySampleInterval = 5 * time.Second

// sampleMemory measures the runner every memorySampleInterval until the
// returned function is called, reporting its use to the grant. The function
// stops sampling, takes a last sample and returns the peaks, and whether any
// sample succeeded. A runner that cannot be measured is reported as using its
// whole reservation, which leaves the budget's floor check to MemAvailable.
func (s *Scaler) sampleMemory(sampler MemorySampler, name string, grant *MemoryGrant) func() (MemorySample, bool) {
	if sampler == nil {
		grant.Observe(grant.mib)
		return func() (MemorySample, bool) { return MemorySample{}, false }
	}
	var peak MemorySample
	measured, failed := false, false
	sample := func() {
		ctx, cancel := context.WithTimeout(context.Background(), memorySampleInterval)
		defer cancel()
		m, err := sampler.SampleMemory(ctx, name)
		if err != nil {
			if !measured && !failed {
				failed = true
				grant.Observe(grant.mib)
				s.logger.Warn("Cannot measure runner memory", slog.String("name", name), slog.String("error", err.Error()))
			}
			return
		}
		measured = true
		peak.ResidentMB = max(peak.ResidentMB, m.ResidentMB)
		peak.TotalMB = max(peak.TotalMB, m.TotalMB)
		grant.Observe(m.ResidentMB)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(memorySampleInterval)
		defer ticker.Stop()
		for {
			sample()
			select {
			case <-stop:
				sample()
				return
			case <-ticker.C:
			}
		}
	}()
	return func() (MemorySample, bool) {
		close(stop)
		<-done
		return peak, measured
	}
}

// logMemoryPeak records a finished job's memory peak beside its reservation,
// so reservations can follow measured use.
func (s *Scaler) logMemoryPeak(name, job, workflow string, reservation int, peak MemorySample) {
	attrs := []any{
		slog.String("name", name),
		slog.String("job", job),
		slog.String("workflow", workflow),
		slog.Int("reservedMB", reservation),
		slog.Int("peakMB", peak.ResidentMB),
		slog.Int("peakWithCacheMB", peak.TotalMB),
	}
	if reservation > 0 && peak.ResidentMB > reservation {
		s.logger.Warn("Runner memory peak exceeded its reservation", attrs...)
		return
	}
	s.logger.Info("Runner memory peak", attrs...)
}

func (s *Scaler) removeRunner(state *RunnerState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runners, state.Name)
	if state.servedJob {
		s.refillLocked()
	}
}

// refillTimeout bounds the JIT request a refill makes while holding s.mu.
const refillTimeout = 30 * time.Second

// refillLocked serves jobs assigned while a finished runner still held the
// last slot. The listener only rescales when a message arrives, and GitHub's
// long poll can hold the next one for about 50s, so without this such a job
// waits out the poll. Runners that failed to provision never refill: the next
// listener message retries them, so a broken image cannot spin. Callers hold
// s.mu inside a lifecycle goroutine, so the wait group is positive at Add.
func (s *Scaler) refillLocked() {
	if s.lifecycleCtx.Err() != nil {
		return
	}
	if min(s.maxRunners, s.pendingJobsLocked()) <= len(s.runners) {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.lifecycleCtx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(s.lifecycleCtx, refillTimeout)
		defer cancel()
		if _, err := s.scaleLocked(ctx, s.pendingJobsLocked()); err != nil {
			// The next listener message retries with fresh statistics.
			s.logger.Warn("Refill after cleanup failed", slog.String("error", err.Error()))
		}
	}()
}

// pendingJobsLocked is the latest assigned-job count less the jobs completed
// since. Callers hold s.mu.
func (s *Scaler) pendingJobsLocked() int {
	return max(0, s.assignedJobs-s.completedSinceCount)
}

func (s *Scaler) deregisterRunner(name string, runnerID int) {
	if runnerID == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.client.RemoveRunner(ctx, int64(runnerID)); err != nil {
		s.logger.Warn("Failed to deregister runner",
			slog.String("name", name),
			slog.Int("runnerID", runnerID),
			slog.String("error", err.Error()),
		)
	}
}
