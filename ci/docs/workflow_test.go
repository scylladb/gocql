// Copyright (C) 2026 ScyllaDB

package docs

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

type workflow struct {
	Permissions map[string]string
	Jobs        map[string]job
}

type job struct {
	Uses        string
	If          string
	Needs       any
	Permissions map[string]string
	With        map[string]any
	Environment any
	Steps       []struct {
		Uses string
		With map[string]any
	}
}

func readWorkflow(t *testing.T, name string) workflow {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	var result workflow
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func requireReadOnly(t *testing.T, permissions map[string]string) {
	t.Helper()
	for permission, access := range permissions {
		if access == "write" {
			t.Errorf("validation requests %s: write", permission)
		}
	}
}

func requireReadOnlyWorkflow(t *testing.T, name string, seen map[string]bool) {
	t.Helper()
	if seen[name] {
		t.Fatalf("recursive workflow call to %s", name)
	}
	seen[name] = true
	defer delete(seen, name)
	w := readWorkflow(t, name)
	requireReadOnly(t, w.Permissions)
	// GitHub validates every nested job's permissions, including skipped jobs.
	for _, j := range w.Jobs {
		requireReadOnly(t, j.Permissions)
		for _, step := range j.Steps {
			if strings.HasPrefix(step.Uses, "actions/deploy-pages@") {
				t.Errorf("validation reaches Pages deployment in %s", name)
			}
		}
		if strings.HasPrefix(j.Uses, "./.github/workflows/") {
			requireReadOnlyWorkflow(t, strings.TrimPrefix(j.Uses, "./.github/workflows/"), seen)
		}
	}
}

func TestDocumentationValidationIsReadOnly(t *testing.T) {
	for _, caller := range []struct{ file, job, target string }{
		{"docs-pr.yml", "build", "${{ github.sha }}"},
		{"release.yml", "docs-precheck", "${{ needs.preflight.outputs.resolved_sha }}"},
	} {
		t.Run(caller.file, func(t *testing.T) {
			w := readWorkflow(t, caller.file)
			j := w.Jobs[caller.job]
			requireReadOnly(t, w.Permissions)
			requireReadOnly(t, j.Permissions)
			requireReadOnlyWorkflow(t, "docs.yml", map[string]bool{})
			if j.Uses != "./.github/workflows/docs.yml" || j.With["package-pages"] != false || j.With["target"] != caller.target {
				t.Fatalf("validation must check its candidate with the read-only builder: %+v", j)
			}
		})
	}
}

func TestDocumentationPublishingCallers(t *testing.T) {
	for _, caller := range []struct{ file, job, target string }{
		{"docs-pages.yml", "publish", "${{ needs.select-target.outputs.target }}"},
		{"release.yml", "publish-docs", "${{ needs.preflight.outputs.root_tag }}"},
	} {
		t.Run(caller.file, func(t *testing.T) {
			j := readWorkflow(t, caller.file).Jobs[caller.job]
			if j.Uses != "./.github/workflows/docs-publish.yml" || j.With["target"] != caller.target {
				t.Fatalf("publishing must reach the deployment workflow with its selected target: %+v", j)
			}
			want := map[string]string{"contents": "read", "pages": "write", "id-token": "write"}
			if !reflect.DeepEqual(j.Permissions, want) {
				t.Errorf("publishing permissions = %v, want %v", j.Permissions, want)
			}
			if caller.file == "release.yml" {
				if j.If != "inputs.mode == 'publish'" || !reflect.DeepEqual(j.Needs, []any{"preflight", "publish"}) {
					t.Errorf("release docs must wait for publication and stay disabled in validate mode: %+v", j)
				}
			}
		})
	}
}

func TestDocumentationDeploymentWaitsForArtifact(t *testing.T) {
	w := readWorkflow(t, "docs-publish.yml")
	build, deploy := w.Jobs["build"], w.Jobs["deploy"]
	if build.Uses != "./.github/workflows/docs.yml" || build.With["package-pages"] != true || build.With["target"] != "${{ inputs.target }}" {
		t.Fatalf("publishing must prepare the Pages artifact for its target: %+v", build)
	}
	requireReadOnly(t, build.Permissions)
	environment, ok := deploy.Environment.(map[string]any)
	if !ok || deploy.Needs != "build" || deploy.If != "" || environment["name"] != "github-pages" {
		t.Errorf("deployment must require a successful build and use the Pages environment: %+v", deploy)
	}
	want := map[string]string{"contents": "read", "pages": "write", "id-token": "write"}
	if !reflect.DeepEqual(deploy.Permissions, want) {
		t.Errorf("deployment permissions = %v, want %v", deploy.Permissions, want)
	}
	deployments := 0
	for _, step := range deploy.Steps {
		if strings.HasPrefix(step.Uses, "actions/deploy-pages@") {
			deployments++
		}
	}
	if deployments != 1 {
		t.Errorf("publishing reaches %d Pages deployments, want 1", deployments)
	}
	uploads := 0
	for _, j := range readWorkflow(t, "docs.yml").Jobs {
		for _, step := range j.Steps {
			if strings.HasPrefix(step.Uses, "actions/upload-artifact@") && step.With["name"] == "github-pages" {
				uploads++
			}
		}
	}
	if uploads != 1 {
		t.Errorf("builder uploads %d Pages artifacts, want 1", uploads)
	}
}
