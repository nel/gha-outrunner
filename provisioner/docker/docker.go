package docker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"

	outrunner "github.com/NetwindHQ/gha-outrunner"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

// Provisioner creates ephemeral Docker containers as GitHub Actions runners.
type Provisioner struct {
	logger *slog.Logger
	client *client.Client
	// procRoot and cgroupRoot are where SampleMemory reads processes and
	// cgroups.
	procRoot, cgroupRoot string

	mu      sync.Mutex
	sampled map[string]*sampledRunner
}

func New(logger *slog.Logger) (*Provisioner, error) {
	opts := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}

	// If DOCKER_HOST isn't set, ask the docker CLI for the active context's endpoint.
	if os.Getenv("DOCKER_HOST") == "" {
		if host := dockerHostFromContext(); host != "" {
			logger.Info("Auto-detected Docker host", slog.String("host", host))
			opts = append(opts, client.WithHost(host))
		}
	}

	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}

	return &Provisioner{
		logger:     logger,
		client:     cli,
		procRoot:   "/proc",
		cgroupRoot: "/sys/fs/cgroup",
		sampled:    make(map[string]*sampledRunner),
	}, nil
}

// dockerHostFromContext asks the docker CLI for the active context's endpoint.
// Works with Colima, Docker Desktop, Podman, etc.
func dockerHostFromContext() string {
	out, err := exec.Command("docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
	if err != nil {
		return ""
	}
	host := strings.TrimSpace(string(out))
	if host == "" || host == "<no value>" {
		return ""
	}
	// Verify the socket actually exists
	if strings.HasPrefix(host, "unix://") {
		if _, err := os.Stat(strings.TrimPrefix(host, "unix://")); err != nil {
			return ""
		}
	}
	return host
}

func (d *Provisioner) Start(ctx context.Context, req *outrunner.RunnerRequest) error {
	if req.Runner == nil || req.Runner.Docker == nil {
		return fmt.Errorf("no docker config for runner %s", req.Name)
	}
	dcfg := req.Runner.Docker
	img := dcfg.Image

	runnerCmd := dcfg.RunnerCmd
	if runnerCmd == "" {
		runnerCmd = "./run.sh"
	}

	// Pull image only if not available locally
	_, err := d.client.ImageInspect(ctx, img)
	if err != nil {
		d.logger.Debug("Pulling image", slog.String("image", img))
		reader, pullErr := d.client.ImagePull(ctx, img, image.PullOptions{})
		if pullErr != nil {
			return fmt.Errorf("pull image: %w", pullErr)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			return fmt.Errorf("drain image pull: %w", err)
		}
		_ = reader.Close()
	}

	resp, err := d.client.ContainerCreate(ctx,
		&container.Config{
			Image: img,
			Cmd:   []string{runnerCmd, "--jitconfig", req.JITConfig},
			Labels: map[string]string{
				"outrunner":      "true",
				"outrunner.name": req.Name,
			},
		},
		hostConfig(dcfg),
		nil, nil, req.Name,
	)
	if err != nil {
		return fmt.Errorf("create container: %w", err)
	}

	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("start container: %w", err)
	}

	label, err := instanceLabel(req.JITConfig)
	if err != nil {
		// Only the job's service containers go unmeasured.
		d.logger.Warn("Cannot derive the runner's container label", slog.String("name", req.Name), slog.String("error", err.Error()))
	}
	d.mu.Lock()
	d.sampled[req.Name] = &sampledRunner{label: label, dirs: make(map[string]string)}
	d.mu.Unlock()

	d.logger.Info("Container started",
		slog.String("name", req.Name),
		slog.String("image", img),
		slog.String("id", resp.ID[:12]),
	)
	return nil
}

func (d *Provisioner) Stop(ctx context.Context, name string) error {
	d.logger.Debug("Stopping container", slog.String("name", name))
	d.mu.Lock()
	delete(d.sampled, name)
	d.mu.Unlock()
	err := d.client.ContainerStop(ctx, name, container.StopOptions{})
	if err != nil {
		// Container may already be gone (AutoRemove)
		d.logger.Debug("Container stop returned error (may already be removed)",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
	}
	return nil
}

func (d *Provisioner) Close() error {
	return d.client.Close()
}

// hostConfig builds the container's host settings from the runner config.
func hostConfig(dcfg *outrunner.DockerImage) *container.HostConfig {
	var mounts []mount.Mount
	for _, m := range dcfg.Mounts {
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   m.Source,
			Target:   m.Target,
			ReadOnly: m.ReadOnly,
		})
	}
	var devices []container.DeviceMapping
	for _, dev := range dcfg.Devices {
		devices = append(devices, container.DeviceMapping{
			PathOnHost:        dev,
			PathInContainer:   dev,
			CgroupPermissions: "rwm",
		})
	}
	return &container.HostConfig{
		AutoRemove:  true,
		Mounts:      mounts,
		NetworkMode: container.NetworkMode(dcfg.Network),
		UsernsMode:  container.UsernsMode(dcfg.Userns),
		GroupAdd:    dcfg.GroupAdd,
		Resources: container.Resources{
			Memory:  int64(dcfg.MemoryMB) << 20,
			Devices: devices,
		},
	}
}
