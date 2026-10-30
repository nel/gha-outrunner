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
