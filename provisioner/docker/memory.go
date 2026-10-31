package docker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	outrunner "github.com/NetwindHQ/gha-outrunner"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
)

var _ outrunner.MemorySampler = (*Provisioner)(nil)

// SampleMemory measures a runner's container and the service containers its
// job started, from their cgroups (v2) on this host. The Actions runner labels
// a job's service containers with its instance label (a key with an empty
// value), derived from the .runner file in the runner's JIT configuration.
func (d *Provisioner) SampleMemory(ctx context.Context, name string) (outrunner.MemorySample, error) {
	d.mu.Lock()
	r := d.sampled[name]
	d.mu.Unlock()
	if r == nil {
		return outrunner.MemorySample{}, fmt.Errorf("runner %s was not started here", name)
	}
	ids := []string{name}
	if r.label != "" {
		services, err := d.client.ContainerList(ctx, container.ListOptions{Filters: filters.NewArgs(filters.Arg("label", r.label))})
		if err != nil {
			return outrunner.MemorySample{}, fmt.Errorf("list service containers: %w", err)
		}
		for _, c := range services {
			ids = append(ids, c.ID)
		}
	}
	var sample outrunner.MemorySample
	for i, id := range ids {
		dir, err := r.cgroupDir(ctx, d, id)
		if err == nil {
			var m cgroupMemory
			if m, err = readCgroupMemory(dir); err == nil {
				sample.ResidentMB += int(m.resident >> 20)
				sample.TotalMB += int(m.total >> 20)
				continue
			}
		}
		// A service container may end between the listing and the read.
		if i == 0 {
			return outrunner.MemorySample{}, err
		}
	}
	return sample, nil
}

// sampledRunner is what SampleMemory keeps of a started runner: the instance
// label of its service containers and the cgroups of its containers.
type sampledRunner struct {
	label string
	// dirs is only used by the runner's sampling goroutine.
	dirs map[string]string
}

// cgroupDir finds a running container's cgroup through its init process, once.
// Podman runs the payload in a "container" child of the container's cgroup,
// which holds its limit; the parent is measured then.
func (r *sampledRunner) cgroupDir(ctx context.Context, d *Provisioner, id string) (string, error) {
	if dir, ok := r.dirs[id]; ok {
		return dir, nil
	}
	info, err := d.client.ContainerInspect(ctx, id)
	if err != nil {
		return "", fmt.Errorf("inspect container: %w", err)
	}
	if info.State == nil || info.State.Pid <= 0 {
		return "", fmt.Errorf("container %s is not running", id)
	}
	data, err := os.ReadFile(filepath.Join(d.procRoot, strconv.Itoa(info.State.Pid), "cgroup"))
	if err != nil {
		return "", fmt.Errorf("read cgroup: %w", err)
	}
	path, ok := strings.CutPrefix(strings.TrimSpace(string(data)), "0::")
	if !ok || strings.Contains(path, "\n") {
		return "", fmt.Errorf("container %s is not in a unified (v2) cgroup", id)
	}
	if filepath.Base(path) == "container" {
		path = filepath.Dir(path)
	}
	dir := filepath.Join(d.cgroupRoot, path)
	r.dirs[id] = dir
	return dir, nil
}

type cgroupMemory struct {
	resident, total uint64
}

// readCgroupMemory reads a cgroup's memory in bytes: resident counts
// anonymous, shared, kernel and swapped-out memory, total all of its charged
// memory and swap.
func readCgroupMemory(dir string) (cgroupMemory, error) {
	f, err := os.Open(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return cgroupMemory{}, err
	}
	defer func() { _ = f.Close() }()
	stat := map[string]uint64{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), " ")
		if !ok {
			continue
		}
		if n, err := strconv.ParseUint(value, 10, 64); err == nil {
			stat[key] = n
		}
	}
	if err := scanner.Err(); err != nil {
		return cgroupMemory{}, err
	}
	kernel, ok := stat["kernel"]
	if !ok {
		// Kernels before 5.18 itemize it.
		kernel = stat["kernel_stack"] + stat["pagetables"] + stat["percpu"] + stat["sock"] + stat["slab_unreclaimable"]
	}
	current, err := readUint(filepath.Join(dir, "memory.current"))
	if err != nil {
		return cgroupMemory{}, err
	}
	// No swap accounting reads as no swap.
	swap, _ := readUint(filepath.Join(dir, "memory.swap.current"))
	return cgroupMemory{
		resident: stat["anon"] + stat["shmem"] + kernel + swap,
		total:    current + swap,
	}, nil
}

func readUint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

// instanceLabel is the Actions runner's DockerInstanceLabel for a JIT
// configuration: the first 6 hex digits of the SHA-256 of its lowercased
// .runner file. The runner labels the containers it creates with it.
func instanceLabel(jitConfig string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(jitConfig)
	if err != nil {
		return "", fmt.Errorf("decode JIT config: %w", err)
	}
	var files map[string]string
	if err := json.Unmarshal(data, &files); err != nil {
		return "", fmt.Errorf("parse JIT config: %w", err)
	}
	runner, err := base64.StdEncoding.DecodeString(files[".runner"])
	if err != nil || len(runner) == 0 {
		return "", fmt.Errorf("JIT config has no .runner file")
	}
	// The runner reads the file as text, which drops a UTF-8 byte order mark.
	text := strings.TrimPrefix(string(runner), "\xef\xbb\xbf")
	sum := sha256.Sum256([]byte(strings.ToLower(text)))
	return hex.EncodeToString(sum[:])[:6], nil
}
