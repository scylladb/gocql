package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSynchronizationProfilesRequireDiagnosticPrefix(t *testing.T) {
	if _, err := startProfiles("", true); err == nil {
		t.Fatal("enabled synchronization sampling without a diagnostic prefix")
	}
}

func TestSynchronizationProfileArtifacts(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "diagnostic")
	profiles, err := startProfiles(prefix, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := profiles.stop(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"cpu.pprof", "heap.before.pprof", "heap.after.pprof", "block.pprof", "mutex.pprof"} {
		info, err := os.Stat(prefix + "." + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() == 0 {
			t.Fatal("empty profile", suffix)
		}
	}
}
