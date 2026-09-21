package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVersions(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"1.0.0", "1.20.0", "1.20.0-rc.1", "1.20.0-alpha-beta.2", "1.20.0-999999999999999999999999999"} {
		version := version
		t.Run("valid/"+version, func(t *testing.T) {
			t.Parallel()
			if _, err := parseVersion(version); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, version := range []string{"", "v1.20.0", "2.0.0", "0.20.0", "1.2", "1.2.3.4", "1.02.3", "1.2.03", "1.2.3+build.1", "1.2.3-rc.01", "1.2.3-", "1.2.3-rc/1", "1.2.3;touch-pwned", "1.2.3-$(id)", "1.2.3-rc\nnext=value"} {
		version := version
		t.Run("invalid/"+strings.ReplaceAll(version, "/", "_"), func(t *testing.T) {
			t.Parallel()
			if _, err := parseVersion(version); err == nil {
				t.Fatalf("%q accepted", version)
			}
		})
	}
}

func TestModuleMappings(t *testing.T) {
	t.Parallel()
	tests := []struct{ module, path, dir, tag, title string }{
		{"root", rootModulePath, ".", "v1.20.0", "v1.20.0"},
		{"lz4", lz4ModulePath, "lz4", "lz4/v1.20.0", "lz4 v1.20.0"},
	}
	for _, tt := range tests {
		got, err := newCandidate(tt.module, "1.20.0")
		if err != nil || got.modulePath != tt.path || got.directory != tt.dir || got.tag != tt.tag || got.title != tt.title {
			t.Fatalf("mapping %#v, %v", got, err)
		}
	}
	if _, err := newCandidate("root; echo pwned", "1.20.0"); err == nil {
		t.Fatal("shell-shaped module accepted")
	}
}

func TestCompareVersions(t *testing.T) {
	ordered := []string{"1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta", "1.0.0-beta.2", "1.0.0-beta.11", "1.0.0-rc.1", "1.0.0", "1.1.0"}
	for i := 1; i < len(ordered); i++ {
		a, _ := parseVersion(ordered[i-1])
		b, _ := parseVersion(ordered[i])
		if compareVersions(a, b) >= 0 {
			t.Fatalf("%s !< %s", ordered[i-1], ordered[i])
		}
	}
}

func TestSigningIdentity(t *testing.T) {
	output := strings.Join([]string{"uid", "", "", "", "", "", "", "", "", "Release\\x20Signer\\x20\\x3crelease@example.com\\x3e"}, ":")
	name, email, err := signingIdentity(output)
	if err != nil || name != "Release Signer" || email != "release@example.com" {
		t.Fatalf("%q <%s>: %v", name, email, err)
	}
}

type fakeTargetGit struct {
	fetchErr, resolveErr, ancestorErr, checkoutErr error
	resolved                                       string
	resolvedRef                                    string
	ancestor                                       bool
	checkedOut                                     string
}

func (f *fakeTargetGit) fetchMaster(context.Context) error { return f.fetchErr }
func (f *fakeTargetGit) resolve(_ context.Context, ref string) (string, error) {
	f.resolvedRef = ref
	return f.resolved, f.resolveErr
}
func (f *fakeTargetGit) isAncestor(context.Context, string) (bool, error) {
	return f.ancestor, f.ancestorErr
}
func (f *fakeTargetGit) checkout(_ context.Context, sha string) error {
	f.checkedOut = sha
	return f.checkoutErr
}

func TestResolveTarget(t *testing.T) {
	sha := strings.Repeat("a", 40)
	tests := []struct {
		name, requested, wantResolve string
		git                          fakeTargetGit
		wantErr                      string
	}{
		{"ok", sha, sha, fakeTargetGit{resolved: sha, ancestor: true}, ""},
		{"master", "master", "refs/remotes/origin/master", fakeTargetGit{resolved: sha, ancestor: true}, ""},
		{"malformed", "master; touch pwned", "", fakeTargetGit{}, "40-character"},
		{"fetch failure", sha, "", fakeTargetGit{fetchErr: errors.New("network down")}, "fetch origin/master"},
		{"resolve failure", sha, sha, fakeTargetGit{resolveErr: errors.New("git unavailable")}, "resolve target"},
		{"different resolution", sha, sha, fakeTargetGit{resolved: strings.Repeat("b", 40)}, "want exact commit"},
		{"non ancestor", sha, sha, fakeTargetGit{resolved: sha}, "not reachable"},
		{"ancestry failure", sha, sha, fakeTargetGit{resolved: sha, ancestorErr: errors.New("broken graph")}, "check target ancestry"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTarget(context.Background(), &tt.git, tt.requested)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("%v; want %s", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != sha || tt.git.checkedOut != sha || tt.git.resolvedRef != tt.wantResolve {
				t.Fatalf("%q %v resolve=%q checkout=%q", got, err, tt.git.resolvedRef, tt.git.checkedOut)
			}
		})
	}
}

func TestValidateReleaseRequest(t *testing.T) {
	t.Parallel()
	root, _ := newCandidate("root", "1.20.0")
	lz4, _ := newCandidate("lz4", "1.20.0")
	tests := []struct {
		name, mode, confirmation string
		candidate                candidate
		wantErr                  bool
	}{
		{"validate", "validate", "", root, false},
		{"validate ignores confirmation", "validate", "stale", root, false},
		{"publish root", "publish", "v1.20.0", root, false},
		{"publish lz4", "publish", "lz4/v1.20.0", lz4, false},
		{"publish empty", "publish", "", root, true},
		{"publish wrong module tag", "publish", "v1.20.0", lz4, true},
		{"unknown mode", "schedule", "v1.20.0", root, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReleaseRequest(tt.candidate, tt.mode, tt.confirmation)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr=%t", err, tt.wantErr)
			}
		})
	}
}

type fakeAPI struct {
	blocker       *blocker
	blockerErr    error
	tagInfo       tagInfo
	tagExists     bool
	tagErr        error
	releaseInfo   releaseInfo
	releaseExists bool
	releaseErr    error
	latestInfo    releaseInfo
	latestExists  bool
	latestErr     error
}

func (f fakeAPI) openBlocker(context.Context) (*blocker, error) { return f.blocker, f.blockerErr }
func (f fakeAPI) tag(context.Context, string) (tagInfo, bool, error) {
	return f.tagInfo, f.tagExists, f.tagErr
}
func (f fakeAPI) release(context.Context, string) (releaseInfo, bool, error) {
	return f.releaseInfo, f.releaseExists, f.releaseErr
}
func (f fakeAPI) latestRelease(context.Context) (releaseInfo, bool, error) {
	return f.latestInfo, f.latestExists, f.latestErr
}

type fakeVerifier struct{ err error }

func (f fakeVerifier) verifyRemoteTag(string) error { return f.err }

func TestReleaseStates(t *testing.T) {
	c, _ := newCandidate("root", "1.20.0")
	sha := strings.Repeat("a", 40)
	goodTag := tagInfo{Target: sha, Annotated: true, Verified: true}
	goodRelease := releaseInfo{ID: 20, TagName: c.tag, Name: c.title}
	tests := []struct {
		name    string
		api     fakeAPI
		verify  error
		want    releaseAction
		wantErr string
	}{
		{"nothing", fakeAPI{}, nil, actionCreateBoth, ""},
		{"tag only", fakeAPI{tagInfo: goodTag, tagExists: true}, nil, actionCreateRelease, ""},
		{"complete", fakeAPI{tagInfo: goodTag, tagExists: true, releaseInfo: goodRelease, releaseExists: true, latestInfo: goodRelease, latestExists: true}, nil, actionComplete, ""},
		{"release without tag", fakeAPI{releaseInfo: goodRelease, releaseExists: true}, nil, "", "without its tag"},
		{"lightweight", fakeAPI{tagInfo: tagInfo{Target: sha}, tagExists: true}, nil, "", "lightweight"},
		{"unverified", fakeAPI{tagInfo: tagInfo{Target: sha, Annotated: true}, tagExists: true}, nil, "", "verified signature"},
		{"wrong target", fakeAPI{tagInfo: tagInfo{Target: strings.Repeat("b", 40), Annotated: true, Verified: true}, tagExists: true}, nil, "", "want " + sha},
		{"wrong signer", fakeAPI{tagInfo: goodTag, tagExists: true}, errors.New("wrong fingerprint"), "", "wrong fingerprint"},
		{"metadata", fakeAPI{tagInfo: goodTag, tagExists: true, releaseInfo: releaseInfo{TagName: c.tag, Name: "wrong"}, releaseExists: true}, nil, "", "conflicting metadata"},
		{"latest", fakeAPI{tagInfo: goodTag, tagExists: true, releaseInfo: goodRelease, releaseExists: true}, nil, "", "latest status"},
		{"tag API", fakeAPI{tagErr: errors.New("tag API down")}, nil, "", "tag API down"},
		{"release API", fakeAPI{tagInfo: goodTag, tagExists: true, releaseErr: errors.New("release API down")}, nil, "", "release API down"},
		{"latest API", fakeAPI{tagInfo: goodTag, tagExists: true, releaseInfo: goodRelease, releaseExists: true, latestErr: errors.New("latest API down")}, nil, "", "latest API down"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := inspectReleaseState(context.Background(), tt.api, fakeVerifier{tt.verify}, c, sha)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("%v; want %s", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("%q %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestLZ4ReleaseNeverLatest(t *testing.T) {
	c, _ := newCandidate("lz4", "1.20.0-rc.1")
	sha := strings.Repeat("a", 40)
	release := releaseInfo{ID: 20, TagName: c.tag, Name: c.title, Prerelease: true}
	api := fakeAPI{tagInfo: tagInfo{Target: sha, Annotated: true, Verified: true}, tagExists: true, releaseInfo: release, releaseExists: true, latestInfo: releaseInfo{ID: 19}, latestExists: true}
	if got, err := inspectReleaseState(context.Background(), api, fakeVerifier{}, c, sha); err != nil || got != actionComplete {
		t.Fatalf("%q %v", got, err)
	}
}

func TestBlockersFailClosed(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body, wantErr string
	}{
		{"none", 200, `[]`, ""}, {"one", 200, `[{"number":1080,"title":"gate","html_url":"https://example/1080"}]`, "#1080"},
		{"API", 500, `{"message":"boom"}`, "500"}, {"not found", 404, `{}`, "not found"}, {"parse", 200, `{not-json`, "decode"}, {"trailing", 200, `[] true`, "trailing data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer token" {
					t.Error("auth")
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			api, _ := newGitHubAPI(server.URL, "scylladb/gocql", "token")
			api.client = server.Client()
			err := checkBlockers(context.Background(), api)
			if tt.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("%v; want %s", err, tt.wantErr)
			}
		})
	}
}
