package outrunner

import "context"

// RunnerRequest contains everything a provisioner needs to start a runner.
type RunnerRequest struct {
	// Name is a unique identifier for this runner instance.
	Name string

	// JITConfig is the base64-encoded JIT configuration from GitHub.
	// Pass to: ./run.sh --jitconfig <JITConfig>
	JITConfig string

	// Runner is the matched runner configuration.
	Runner *RunnerConfig
}

// Provisioner creates and destroys ephemeral runner environments.
// Each implementation handles a different backend (Docker, libvirt, etc.).
type Provisioner interface {
	// Start provisions a new runner environment and starts the GitHub Actions
	// runner process inside it. The runner should use the JIT config to
	// register itself with GitHub and pick up the assigned job.
	//
	// Start must return after the runner process has been launched.
	// It does not need to wait for the job to complete.
	Start(ctx context.Context, req *RunnerRequest) error

	// Stop tears down the runner environment. Called after the job completes
	// or if the runner needs to be forcefully removed.
	Stop(ctx context.Context, name string) error

	// Close releases any resources held by the provisioner (e.g., Docker client).
	Close() error
}

// MemorySampler is implemented by provisioners that can measure the memory a
// runner's job holds, so its peak is logged and the memory budget's floor
// check counts only the part of its reservation it has not used yet.
type MemorySampler interface {
	// SampleMemory measures the runner's environment and what its job
	// started beside it, such as service containers.
	SampleMemory(ctx context.Context, name string) (MemorySample, error)
}

// MemorySample is a runner's memory use at one moment, in MiB.
type MemorySample struct {
	// ResidentMB is what the host cannot reclaim: anonymous, shared,
	// kernel and swapped-out memory.
	ResidentMB int
	// TotalMB adds the page cache the host can reclaim under pressure.
	TotalMB int
}
