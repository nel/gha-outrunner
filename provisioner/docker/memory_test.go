package docker

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestInstanceLabel(t *testing.T) {
	runner := `{"AgentId":"752","AgentName":"Hoplum-CI-x64-1"}`
	b64 := base64.StdEncoding.EncodeToString
	files, _ := json.Marshal(map[string]string{
		".runner":      b64([]byte("\xef\xbb\xbf" + runner)),
		".credentials": b64([]byte("{}")),
	})
	sum := sha256.Sum256([]byte(`{"agentid":"752","agentname":"hoplum-ci-x64-1"}`))
	want := hex.EncodeToString(sum[:])[:6]
	if got, err := instanceLabel(b64(files)); err != nil || got != want {
		t.Fatalf("instanceLabel = %q, %v; want %q", got, err, want)
	}
	if _, err := instanceLabel(b64([]byte(`{}`))); err == nil {
		t.Fatal("a JIT config without .runner gave a label")
	}
}

func TestReadCgroupMemory(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("memory.stat", "anon 1000\nfile 5000\nkernel 300\nshmem 20\ninactive_file 4000\n")
	write("memory.current", "6400\n")
	write("memory.swap.current", "7\n")
	m, err := readCgroupMemory(dir)
	if err != nil || m.resident != 1327 || m.total != 6407 {
		t.Fatalf("readCgroupMemory = %+v, %v", m, err)
	}

	// Older kernels itemize kernel memory and may not account swap.
	write("memory.stat", "anon 1000\nshmem 20\nkernel_stack 10\npagetables 20\npercpu 1\nsock 2\nslab_unreclaimable 30\nslab_reclaimable 99\n")
	if err := os.Remove(filepath.Join(dir, "memory.swap.current")); err != nil {
		t.Fatal(err)
	}
	if m, err = readCgroupMemory(dir); err != nil || m.resident != 1083 || m.total != 6400 {
		t.Fatalf("readCgroupMemory without kernel = %+v, %v", m, err)
	}
}
