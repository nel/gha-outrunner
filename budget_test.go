package outrunner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

func newBudgetScaler(client ScaleSetClient, prov Provisioner, prefix string, memory int, budget *MemoryBudget) *Scaler {
	return NewScaler(
		noopLogger(),
		client, 1, 10, prefix,
		&RunnerConfig{Docker: &DockerImage{Image: "test:latest", MemoryMB: memory}},
		prov,
	).WithMemoryBudget(budget)
}

func (m *mockProvisioner) startedNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.started...)
}

func waitFor(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func complete(t *testing.T, s *Scaler, name string) {
	t.Helper()
	_ = s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: name})
	_ = s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: name, Result: "succeeded"})
}

// testBudget is a MemoryBudget with a manual clock and host memory, which a
// test reconsiders with tick instead of the retry timer.
type testBudget struct {
	*MemoryBudget
	mu        sync.Mutex
	clock     time.Time
	available int
}

func newTestBudget(cfg MemoryBudgetConfig, available int) *testBudget {
	tb := &testBudget{MemoryBudget: NewMemoryBudget(noopLogger(), cfg), clock: time.Unix(0, 0), available: available}
	tb.retry = time.Hour
	tb.now = func() time.Time { tb.mu.Lock(); defer tb.mu.Unlock(); return tb.clock }
	tb.MemoryBudget.available = func() (int, error) { tb.mu.Lock(); defer tb.mu.Unlock(); return tb.available, nil }
	return tb
}

func (tb *testBudget) advance(d time.Duration) {
	tb.mu.Lock()
	tb.clock = tb.clock.Add(d)
	tb.mu.Unlock()
	tb.tick()
}

func (tb *testBudget) setAvailable(mib int) {
	tb.mu.Lock()
	tb.available = mib
	tb.mu.Unlock()
	tb.tick()
}

func (tb *testBudget) tick() {
	tb.MemoryBudget.mu.Lock()
	defer tb.MemoryBudget.mu.Unlock()
	tb.dispatchLocked()
}

// pending is a runner in Acquire; grant is set once done is closed.
type pending struct {
	done  chan struct{}
	grant *MemoryGrant
}

// acquire starts an Acquire and waits until it has been queued or admitted.
func (tb *testBudget) acquire(t *testing.T, name string, mib int) *pending {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := &pending{done: make(chan struct{})}
	queued := make(chan struct{}, 1)
	go func() {
		defer close(p.done)
		if g, err := tb.Acquire(ctx, name, mib, func() { queued <- struct{}{} }); err == nil {
			p.grant = g
		}
	}()
	select {
	case <-queued:
	case <-p.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s neither waited nor was admitted", name)
	}
	return p
}

// admitted returns the runner's grant once it holds its reservation, or nil.
func (p *pending) admitted(t *testing.T) *MemoryGrant {
	t.Helper()
	select {
	case <-p.done:
		return p.grant
	case <-time.After(20 * time.Millisecond):
		return nil
	}
}

// Two scale sets share one budget: a runner that does not fit waits, without
// blocking the listener, and provisions once another set's runner is stopped.
func TestMemoryBudgetSharedAcrossScaleSets(t *testing.T) {
	budget := NewMemoryBudget(noopLogger(), MemoryBudgetConfig{BudgetMB: 10240})
	client := newMockClient()
	golden, windows := newMockProvisioner(), newMockProvisioner()
	goldenSet := newBudgetScaler(client, golden, "golden", 10240, budget)
	windowsSet := newBudgetScaler(client, windows, "windows", 6144, budget)
	defer goldenSet.Shutdown(context.Background())
	defer windowsSet.Shutdown(context.Background())

	if _, err := goldenSet.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the golden runner", func() bool { return len(golden.startedNames()) == 1 })

	if count, err := windowsSet.HandleDesiredRunnerCount(context.Background(), 2); err != nil || count != 2 {
		t.Fatalf("HandleDesiredRunnerCount = %d, %v", count, err)
	}
	time.Sleep(100 * time.Millisecond)
	if started := windows.startedNames(); len(started) != 0 {
		t.Fatalf("Windows runners started past the budget: %v", started)
	}
	for _, runner := range windowsSet.Runners() {
		if runner.Phase != RunnerProvisioning {
			t.Errorf("waiting runner %s is %s", runner.Name, runner.Phase)
		}
	}

	complete(t, goldenSet, golden.startedNames()[0])
	waitFor(t, "one admitted Windows runner", func() bool { return len(windows.startedNames()) == 1 })
	time.Sleep(100 * time.Millisecond)
	if started := windows.startedNames(); len(started) != 1 {
		t.Fatalf("two 6144 MiB runners fit a 10240 MiB budget: %v", started)
	}

	complete(t, windowsSet, windows.startedNames()[0])
	waitFor(t, "the second Windows runner", func() bool { return len(windows.startedNames()) == 2 })
}

// A small runner that fits is admitted while an older, larger one waits.
func TestMemoryBudgetBackfillsSmallRunners(t *testing.T) {
	tb := newTestBudget(MemoryBudgetConfig{BudgetMB: 8192, Aging: 10 * time.Minute}, 0)
	first := tb.acquire(t, "linux", 4096)
	g := first.admitted(t)
	if g == nil {
		t.Fatal("the first runner waited on an empty budget")
	}
	large := tb.acquire(t, "windows", 6144)
	small := tb.acquire(t, "select", 512)
	if large.admitted(t) != nil {
		t.Fatal("the large runner was admitted past the budget")
	}
	if small.admitted(t) == nil {
		t.Fatal("the small runner waited behind the large one although it fits")
	}
	g.Release()
	if large.admitted(t) == nil {
		t.Fatal("the large runner was not admitted once memory was freed")
	}
}

// A runner waiting past the aging limit takes the next freed memory: later
// runners no longer overtake it, so it cannot starve.
func TestMemoryBudgetAgingStopsBackfill(t *testing.T) {
	tb := newTestBudget(MemoryBudgetConfig{BudgetMB: 8192, Aging: 10 * time.Minute}, 0)
	a := tb.acquire(t, "linux-a", 2048).admitted(t)
	b := tb.acquire(t, "linux-b", 4096).admitted(t)
	large := tb.acquire(t, "windows", 6144)
	tb.advance(10 * time.Minute)

	small := tb.acquire(t, "select", 512)
	if small.admitted(t) != nil {
		t.Fatal("a small runner overtook a runner waiting past the aging limit")
	}
	a.Release()
	if large.admitted(t) != nil {
		t.Fatal("the large runner was admitted past the budget")
	}
	if small.admitted(t) != nil {
		t.Fatal("freed memory went to a later runner instead of the aged one")
	}
	b.Release()
	if large.admitted(t) == nil {
		t.Fatal("the aged runner was not admitted once it fits")
	}
	if small.admitted(t) == nil {
		t.Fatal("the small runner was not admitted after the aged one")
	}
}

// The floor holds a runner back while the host has less memory available
// than its reservation, the reservations running runners have not used yet,
// and the floor.
func TestMemoryBudgetFloorChecksHostMemory(t *testing.T) {
	tb := newTestBudget(MemoryBudgetConfig{BudgetMB: 16384, FloorMB: 1024}, 12288)
	running := tb.acquire(t, "kvm", 6144).admitted(t)
	if running == nil {
		t.Fatal("the first runner waited although the host has room")
	}
	// 12288 available - 1024 floor - 6144 unused by the running runner.
	next := tb.acquire(t, "linux", 6144)
	if next.admitted(t) != nil {
		t.Fatal("a runner was admitted although its reservation would cut into the floor")
	}
	// The running job reaches its reservation, which MemAvailable now shows.
	running.Observe(6144)
	tb.setAvailable(6144)
	if next.admitted(t) != nil {
		t.Fatal("a runner was admitted into memory the host does not have")
	}
	tb.setAvailable(7168)
	if next.admitted(t) == nil {
		t.Fatal("the runner was not admitted once the host had room above the floor")
	}
}

// A runner still waiting for memory at shutdown is deregistered, not started.
func TestMemoryBudgetShutdownWhileWaiting(t *testing.T) {
	budget := NewMemoryBudget(noopLogger(), MemoryBudgetConfig{BudgetMB: 4096})
	held, err := budget.Acquire(context.Background(), "holder", 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	client := newMockClient()
	prov := newMockProvisioner()
	s := newBudgetScaler(client, prov, "test", 4096, budget)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	s.Shutdown(context.Background())
	if started := prov.startedNames(); len(started) != 0 {
		t.Errorf("started while waiting: %v", started)
	}
	if client.removeCount.Load() != 1 {
		t.Errorf("expected 1 RemoveRunner call, got %d", client.removeCount.Load())
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if len(budget.waiting) != 0 || budget.used != 4096 {
		t.Errorf("after shutdown: %d waiting, %d MiB used", len(budget.waiting), budget.used)
	}
}

// samplingProvisioner reports a fixed memory sample for every runner.
type samplingProvisioner struct {
	*mockProvisioner
	sample MemorySample
	err    error
}

func (p *samplingProvisioner) SampleMemory(context.Context, string) (MemorySample, error) {
	return p.sample, p.err
}

// A runner's measured use reaches its grant, so the floor check counts only
// the reservation it does not use. A runner that cannot be measured, or whose
// sampling fails, counts as using its whole reservation once provisioned.
func TestMemoryUseReachesGrant(t *testing.T) {
	for _, tc := range []struct {
		name  string
		prov  Provisioner
		inUse int
	}{
		{"measured", &samplingProvisioner{mockProvisioner: newMockProvisioner(), sample: MemorySample{ResidentMB: 3000, TotalMB: 3500}}, 3000},
		{"no sampler", newMockProvisioner(), 4096},
		{"sampling fails", &samplingProvisioner{mockProvisioner: newMockProvisioner(), err: errors.New("no cgroup")}, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := NewMemoryBudget(noopLogger(), MemoryBudgetConfig{BudgetMB: 8192, FloorMB: 1})
			budget.available = func() (int, error) { return 1 << 20, nil }
			s := newBudgetScaler(newMockClient(), tc.prov, "test", 4096, budget)
			defer s.Shutdown(context.Background())
			if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the runner's use", func() bool {
				budget.mu.Lock()
				defer budget.mu.Unlock()
				for g := range budget.grants {
					return g.inUse == tc.inUse
				}
				return false
			})
		})
	}
}

func TestReadMemAvailable(t *testing.T) {
	if _, err := os.Stat("/proc/meminfo"); err != nil {
		t.Skip("no /proc/meminfo")
	}
	if mib, err := readMemAvailable(); err != nil || mib <= 0 {
		t.Fatalf("readMemAvailable = %d, %v", mib, err)
	}
}

func TestLoadConfigMemoryBudget(t *testing.T) {
	for _, tc := range []struct {
		name, global, runner, err string
	}{
		{"fits", "", "docker: {image: runner:latest, memory: 2048}", ""},
		{"libvirt default", "", "libvirt: {path: /golden.qcow2}", "between 1 and 4096"},
		{"unlimited container", "", "docker: {image: runner:latest}", "between 1 and 4096"},
		{"reservation", "", "memory_reservation: 4096\n    docker: {image: runner:latest}", ""},
		{"reservation past budget", "", "memory_reservation: 4097\n    docker: {image: runner:latest, memory: 2048}", "between 1 and 4096"},
		{"floor and aging", "memory_floor: 1024\nmemory_aging: 10m\n", "docker: {image: runner:latest, memory: 2048}", ""},
		{"negative floor", "memory_floor: -1\n", "docker: {image: runner:latest, memory: 2048}", "must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			content := "url: https://github.com/org/repo\nmemory_budget: 4096\n" + tc.global + "runners:\n  r:\n    labels: [x]\n    " + tc.runner + "\n"
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if tc.err == "" {
				if err != nil || cfg.MemoryBudget != 4096 {
					t.Fatalf("LoadConfig = %v, %v", cfg, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("LoadConfig error = %v, want %q", err, tc.err)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte("url: https://github.com/org/repo\nmemory_aging: 10m\nrunners:\n  r:\n    labels: [x]\n    docker: {image: runner:latest}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("memory_aging without memory_budget was accepted")
	}
}
