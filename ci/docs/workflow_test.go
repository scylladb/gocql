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

func requireReadOnly(t *testing.T, scope string, permissions map[string]string) {
	t.Helper()
	for permission, access := range permissions {
		if access == "write" {
			t.Errorf("%s: read-only workflow requests %s: write", scope, permission)
		}
	}
}

func requirePermissionsWithin(t *testing.T, scope string, permissions, granted map[string]string) {
	t.Helper()
	levels := map[string]int{"": 0, "none": 0, "read": 1, "write": 2}
	for permission, access := range permissions {
		level, valid := levels[access]
		if !valid || level > levels[granted[permission]] {
			t.Errorf("%s: workflow requests %s: %s, caller grants %q", scope, permission, access, granted[permission])
		}
	}
}

func effectivePermissions(workflow, job map[string]string) map[string]string {
	if job != nil {
		return job
	}
	return workflow
}

func requireWorkflowPermissions(t *testing.T, name string, granted map[string]string, readOnly bool, seen map[string]bool) {
	t.Helper()
	if seen[name] {
		t.Fatalf("recursive workflow call to %s", name)
	}
	seen[name] = true
	defer delete(seen, name)
	w := readWorkflow(t, name)
	readOnly = readOnly || name == "docs.yml"
	requirePermissionsWithin(t, name, w.Permissions, granted)
	workflowPermissions := effectivePermissions(granted, w.Permissions)
	if readOnly {
		requireReadOnly(t, name, workflowPermissions)
	}
	// GitHub validates every nested job's permissions, including skipped jobs.
	for jobName, j := range w.Jobs {
		requirePermissionsWithin(t, name+" job "+jobName, j.Permissions, granted)
		jobPermissions := effectivePermissions(workflowPermissions, j.Permissions)
		if readOnly {
			requireReadOnly(t, name+" job "+jobName, jobPermissions)
			for _, step := range j.Steps {
				if strings.HasPrefix(step.Uses, "actions/deploy-pages@") {
					t.Errorf("read-only workflow reaches Pages deployment in %s job %s", name, jobName)
				}
			}
		}
		if strings.HasPrefix(j.Uses, "./.github/workflows/") {
			requireWorkflowPermissions(t, strings.TrimPrefix(j.Uses, "./.github/workflows/"), jobPermissions, readOnly, seen)
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
			requireReadOnly(t, caller.file, w.Permissions)
			requireReadOnly(t, caller.file+" job "+caller.job, j.Permissions)
			requireWorkflowPermissions(t, "docs.yml", effectivePermissions(w.Permissions, j.Permissions), true, map[string]bool{})
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
			requireWorkflowPermissions(t, "docs-publish.yml", j.Permissions, false, map[string]bool{})
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
	if deploy.Uses != "./.github/workflows/docs-deploy.yml" || deploy.Needs != "build" || deploy.If != "" {
		t.Fatalf("deployment must call the shared deployer after a successful build: %+v", deploy)
	}
	deployment := readWorkflow(t, "docs-deploy.yml").Jobs["deploy"]
	environment, ok := deployment.Environment.(map[string]any)
	if !ok || deployment.If != "" || environment["name"] != "github-pages" {
		t.Errorf("deployment must use the Pages environment: %+v", deployment)
	}
	want := map[string]string{"contents": "read", "pages": "write", "id-token": "write"}
	if !reflect.DeepEqual(deploy.Permissions, want) {
		t.Errorf("deployment permissions = %v, want %v", deploy.Permissions, want)
	}
	if !reflect.DeepEqual(deployment.Permissions, want) {
		t.Errorf("shared deployment permissions = %v, want %v", deployment.Permissions, want)
	}
	deployments := 0
	for _, step := range deployment.Steps {
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
