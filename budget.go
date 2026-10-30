package outrunner

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// MemoryBudget admits runners of every scale set against one shared host
// memory budget, so each set's max_runners can follow its demand instead of
// a static split that reserves memory for sets that are usually idle. A
// runner holds its memory from provisioning until its environment is stopped.
// Admission is first come, first served: a large runner waiting at the head
// is not overtaken by smaller ones that would delay it indefinitely.
type MemoryBudget struct {
	sem *semaphore.Weighted
}

// NewMemoryBudget returns a budget of mib MiB, or nil (no limit) when mib is 0.
func NewMemoryBudget(mib int) *MemoryBudget {
	if mib <= 0 {
		return nil
	}
	return &MemoryBudget{sem: semaphore.NewWeighted(int64(mib))}
}

// TryAcquire reserves mib MiB if they are free and no runner is waiting.
func (b *MemoryBudget) TryAcquire(mib int) bool {
	return b == nil || b.sem.TryAcquire(int64(mib))
}

// Acquire waits until mib MiB are free, or ctx ends.
func (b *MemoryBudget) Acquire(ctx context.Context, mib int) error {
	if b == nil {
		return nil
	}
	return b.sem.Acquire(ctx, int64(mib))
}

// Release returns mib MiB to the budget.
func (b *MemoryBudget) Release(mib int) {
	if b != nil {
		b.sem.Release(int64(mib))
	}
}

// WithMemoryBudget makes the scaler admit its runners against budget, which
// the scale sets of one host share.
func (s *Scaler) WithMemoryBudget(budget *MemoryBudget) *Scaler {
	s.budget = budget
	return s
}

// MemoryMB is the memory a runner environment is limited to, which the
// budget counts: the VM's allocation or the container's limit (0 when a
// container is unlimited).
func (r *RunnerConfig) MemoryMB() int {
	switch {
	case r.Docker != nil:
		return r.Docker.MemoryMB
	case r.Libvirt != nil:
		return r.Libvirt.MemoryMB
	case r.Tart != nil:
		return r.Tart.MemoryMB
	default:
		return 0
	}
}
