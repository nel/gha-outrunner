package outrunner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

// Two scale sets share one budget: a runner that does not fit waits, without
// blocking the listener, and provisions once another set's runner is stopped.
func TestMemoryBudgetSharedAcrossScaleSets(t *testing.T) {
	budget := NewMemoryBudget(10240)
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

// A waiting large runner is not overtaken by smaller ones that arrive later.
func TestMemoryBudgetFirstComeFirstServed(t *testing.T) {
	budget := NewMemoryBudget(8192)
	client := newMockClient()
	small, large := newMockProvisioner(), newMockProvisioner()
	smallSet := newBudgetScaler(client, small, "small", 2048, budget)
	largeSet := newBudgetScaler(client, large, "large", 8192, budget)
	defer smallSet.Shutdown(context.Background())
	defer largeSet.Shutdown(context.Background())

	if _, err := smallSet.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first small runner", func() bool { return len(small.startedNames()) == 1 })
	if _, err := largeSet.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := smallSet.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if started := small.startedNames(); len(started) != 1 {
		t.Fatalf("a small runner overtook the waiting large one: %v", started)
	}

	complete(t, smallSet, small.startedNames()[0])
	waitFor(t, "the large runner", func() bool { return len(large.startedNames()) == 1 })
	complete(t, largeSet, large.startedNames()[0])
	waitFor(t, "the second small runner", func() bool { return len(small.startedNames()) == 2 })
}

// A runner still waiting for memory at shutdown is deregistered, not started.
func TestMemoryBudgetShutdownWhileWaiting(t *testing.T) {
	budget := NewMemoryBudget(4096)
	if !budget.TryAcquire(4096) {
		t.Fatal("empty budget refused its whole size")
	}
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
}

func TestLoadConfigMemoryBudget(t *testing.T) {
	for _, tc := range []struct {
		name, runner, err string
	}{
		{"fits", "docker: {image: runner:latest, memory: 2048}", ""},
		{"libvirt default", "libvirt: {path: /golden.qcow2}", "between 1 and 4096"},
		{"unlimited container", "docker: {image: runner:latest}", "between 1 and 4096"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			content := "url: https://github.com/org/repo\nmemory_budget: 4096\nrunners:\n  r:\n    labels: [x]\n    " + tc.runner + "\n"
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
}
