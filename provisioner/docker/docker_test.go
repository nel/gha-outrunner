package docker

import (
	"testing"

	outrunner "github.com/NetwindHQ/gha-outrunner"
)

func TestHostConfigNetwork(t *testing.T) {
	tests := []struct {
		network string
		want    string
	}{
		{"", ""}, // runtime default
		{"host", "host"},
		{"ci-net", "ci-net"},
	}
	for _, tt := range tests {
		hc := hostConfig(&outrunner.DockerImage{Image: "runner:latest", Network: tt.network})
		if string(hc.NetworkMode) != tt.want {
			t.Errorf("network %q: NetworkMode = %q, want %q", tt.network, hc.NetworkMode, tt.want)
		}
		if !hc.AutoRemove {
			t.Errorf("network %q: AutoRemove off", tt.network)
		}
	}
}

func TestHostConfigMounts(t *testing.T) {
	hc := hostConfig(&outrunner.DockerImage{
		Image:  "runner:latest",
		Mounts: []outrunner.DockerMount{{Source: "/var/cache/vcpkg", Target: "/opt/vcpkg-cache", ReadOnly: true}},
	})
	if len(hc.Mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(hc.Mounts))
	}
	m := hc.Mounts[0]
	if m.Source != "/var/cache/vcpkg" || m.Target != "/opt/vcpkg-cache" || !m.ReadOnly {
		t.Errorf("unexpected mount: %+v", m)
	}
}

func TestHostConfigUsernsAndMemory(t *testing.T) {
	hc := hostConfig(&outrunner.DockerImage{Image: "runner:latest"})
	if hc.UsernsMode != "" || hc.Memory != 0 {
		t.Errorf("defaults: UsernsMode = %q, Memory = %d, want both unset", hc.UsernsMode, hc.Memory)
	}

	hc = hostConfig(&outrunner.DockerImage{
		Image:    "runner:latest",
		Userns:   "keep-id:uid=1001,gid=1001",
		MemoryMB: 2048,
	})
	if hc.UsernsMode != "keep-id:uid=1001,gid=1001" {
		t.Errorf("UsernsMode = %q", hc.UsernsMode)
	}
	if hc.Memory != 2147483648 {
		t.Errorf("Memory = %d, want 2147483648", hc.Memory)
	}
}
