package outrunner

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MemoryBudget admits runners of every scale set against one shared host
// memory budget, so each set's max_runners can follow its demand instead of
// a static split that reserves memory for sets that are usually idle. A
// runner holds its reservation from provisioning until its environment is
// stopped.
//
// Waiting runners are admitted in arrival order, but a runner that fits is
// admitted even when an older, larger one does not (backfill), so short small
// jobs do not queue behind a large one waiting for memory. A runner that has
// waited longer than the aging limit takes the next freed memory: nothing that
// arrived after it is admitted until it is, so it cannot starve.
//
// With a floor, a runner is also admitted only if the host's MemAvailable,
// less the part of running runners' reservations they do not use now, keeps
// the floor free once its own reservation is counted. A reservation below a
// job's real use then delays the next jobs instead of letting the host run
// out. A runner counts its whole reservation as unused until its use is first
// observed; one that cannot be measured is observed as using all of it once
// provisioned, so only MemAvailable shows what it holds.
type MemoryBudget struct {
	logger *slog.Logger
	budget int
	floor  int
	aging  time.Duration
	// available reads MemAvailable in MiB; now and retry are the clock and
	// the interval at which waiting runners are reconsidered without a
	// release (aging and the floor change with time).
	available func() (int, error)
	now       func() time.Time
	retry     time.Duration

	mu       sync.Mutex
	used     int
	waiting  []*memoryWaiter
	grants   map[*MemoryGrant]struct{}
	retrying bool
	// guardErr is logged once when MemAvailable cannot be read.
	guardErr bool
}

// MemoryBudgetConfig sizes a MemoryBudget, in MiB.
type MemoryBudgetConfig struct {
	BudgetMB int
	// FloorMB of MemAvailable is kept free; 0 disables the host check.
	FloorMB int
	// Aging is how long a runner may be overtaken; 0 never stops backfill.
	Aging time.Duration
}

type memoryWaiter struct {
	name  string
	mib   int
	since time.Time
	// aged and floorHeld are logged once.
	aged, floorHeld bool
	grant           *MemoryGrant
	ready           chan struct{}
}

// MemoryGrant is a runner's admitted reservation.
type MemoryGrant struct {
	budget *MemoryBudget
	mib    int
	// inUse is the runner's last observed use, which the floor check
	// subtracts from the reservation; 0 until first observed.
	inUse int
}

// memoryRetry is how often waiting runners are reconsidered between releases.
const memoryRetry = 5 * time.Second

// NewMemoryBudget returns a budget, or nil (no limit) when its size is 0.
func NewMemoryBudget(logger *slog.Logger, cfg MemoryBudgetConfig) *MemoryBudget {
	if cfg.BudgetMB <= 0 {
		return nil
	}
	return &MemoryBudget{
		logger:    logger,
		budget:    cfg.BudgetMB,
		floor:     cfg.FloorMB,
		aging:     cfg.Aging,
		available: readMemAvailable,
		now:       time.Now,
		retry:     memoryRetry,
		grants:    make(map[*MemoryGrant]struct{}),
	}
}

// Acquire waits until mib MiB are admitted for the runner name, or ctx ends.
// It calls onWait once if the runner cannot be admitted at once.
func (b *MemoryBudget) Acquire(ctx context.Context, name string, mib int, onWait func()) (*MemoryGrant, error) {
	if b == nil {
		return &MemoryGrant{mib: mib}, nil
	}
	w := &memoryWaiter{name: name, mib: mib, since: b.now(), ready: make(chan struct{})}
	b.mu.Lock()
	b.waiting = append(b.waiting, w)
	b.dispatchLocked()
	admitted := w.grant != nil
	b.mu.Unlock()
	if !admitted && onWait != nil {
		onWait()
	}
	select {
	case <-w.ready:
		return w.grant, nil
	case <-ctx.Done():
		b.mu.Lock()
		defer b.mu.Unlock()
		if w.grant != nil {
			b.releaseLocked(w.grant)
		} else {
			b.removeWaiterLocked(w)
			b.dispatchLocked()
		}
		return nil, ctx.Err()
	}
}

// Observe records the runner's current memory use in MiB.
func (g *MemoryGrant) Observe(mib int) {
	if g == nil || g.budget == nil {
		return
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	g.inUse = mib
}

// Release returns the reservation to the budget.
func (g *MemoryGrant) Release() {
	if g == nil || g.budget == nil {
		return
	}
	g.budget.mu.Lock()
	defer g.budget.mu.Unlock()
	g.budget.releaseLocked(g)
}

func (b *MemoryBudget) releaseLocked(g *MemoryGrant) {
	if _, held := b.grants[g]; !held {
		return
	}
	delete(b.grants, g)
	b.used -= g.mib
	b.dispatchLocked()
}

func (b *MemoryBudget) removeWaiterLocked(w *memoryWaiter) {
	for i, other := range b.waiting {
		if other == w {
			b.waiting = append(b.waiting[:i], b.waiting[i+1:]...)
			return
		}
	}
}

// dispatchLocked admits every waiting runner that fits, oldest first, and
// stops at the oldest one past the aging limit that does not.
func (b *MemoryBudget) dispatchLocked() {
	now := b.now()
	headroom, guarded := b.headroomLocked()
	for i := 0; i < len(b.waiting); {
		w := b.waiting[i]
		fits := b.used+w.mib <= b.budget
		if fits && guarded && w.mib > headroom && !w.floorHeld {
			w.floorHeld = true
			b.logger.Warn("Host memory floor holds a runner the budget admits",
				slog.String("name", w.name),
				slog.Int("memoryMB", w.mib),
				slog.Int("headroomMB", headroom),
				slog.Int("floorMB", b.floor),
			)
		}
		if fits && (!guarded || w.mib <= headroom) {
			g := &MemoryGrant{budget: b, mib: w.mib}
			b.grants[g] = struct{}{}
			b.used += w.mib
			headroom -= w.mib
			w.grant = g
			close(w.ready)
			b.waiting = append(b.waiting[:i], b.waiting[i+1:]...)
			continue
		}
		if b.aging > 0 && now.Sub(w.since) >= b.aging {
			if !w.aged {
				w.aged = true
				b.logger.Info("Reserving freed host memory for a long-waiting runner",
					slog.String("name", w.name),
					slog.Int("memoryMB", w.mib),
					slog.Int("usedMB", b.used),
				)
			}
			break
		}
		i++
	}
	if len(b.waiting) > 0 && !b.retrying {
		b.retrying = true
		time.AfterFunc(b.retry, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.retrying = false
			b.dispatchLocked()
		})
	}
}

// headroomLocked is the memory the host can still give new runners above the
// floor: MemAvailable less the floor and the part of running runners'
// reservations they do not use now. guarded is false when there is no floor
// or MemAvailable cannot be read.
func (b *MemoryBudget) headroomLocked() (headroom int, guarded bool) {
	if b.floor <= 0 {
		return 0, false
	}
	available, err := b.available()
	if err != nil {
		if !b.guardErr {
			b.guardErr = true
			b.logger.Warn("Cannot read host memory; admitting on the budget alone", slog.String("error", err.Error()))
		}
		return 0, false
	}
	headroom = available - b.floor
	for g := range b.grants {
		headroom -= max(0, g.mib-g.inUse)
	}
	return headroom, true
}

// readMemAvailable returns the kernel's estimate of the memory available for
// new work without swapping, in MiB.
func readMemAvailable() (int, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemAvailable:" {
			kib, err := strconv.Atoi(fields[1])
			if err != nil {
				return 0, fmt.Errorf("parse MemAvailable: %w", err)
			}
			return kib / 1024, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("no MemAvailable in /proc/meminfo")
}

// WithMemoryBudget makes the scaler admit its runners against budget, which
// the scale sets of one host share.
func (s *Scaler) WithMemoryBudget(budget *MemoryBudget) *Scaler {
	s.budget = budget
	return s
}

// MemoryMB is the memory a runner environment is limited to: the VM's
// allocation or the container's limit (0 when a container is unlimited).
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

// ReservationMB is the memory the budget counts for one runner: its
// memory_reservation, which covers what its job starts beside the
// environment (such as service containers), or else its memory limit.
func (r *RunnerConfig) ReservationMB() int {
	if r.MemoryReservation > 0 {
		return r.MemoryReservation
	}
	return r.MemoryMB()
}
