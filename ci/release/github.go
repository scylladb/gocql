package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type blocker struct {
	Title, HTMLURL string
	Number         int
}
type tagInfo struct {
	Target              string
	Annotated, Verified bool
}
type releaseInfo struct {
	TagName    string `json:"tag_name"`
	Name       string `json:"name"`
	ID         int64  `json:"id"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
}

type releaseAPI interface {
	openBlocker(context.Context) (*blocker, error)
	tag(context.Context, string) (tagInfo, bool, error)
	release(context.Context, string) (releaseInfo, bool, error)
	latestRelease(context.Context) (releaseInfo, bool, error)
}

type githubAPI struct {
	client               *http.Client
	baseURL, repo, token string
}

func newGitHubAPI(baseURL, repo, token string) (*githubAPI, error) {
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	if repo == "" || strings.Count(repo, "/") != 1 {
		return nil, fmt.Errorf("GITHUB_REPOSITORY must be owner/name, got %q", repo)
	}
	if token == "" {
		return nil, fmt.Errorf("a GitHub API token is required")
	}
	return &githubAPI{client: http.DefaultClient, baseURL: strings.TrimRight(baseURL, "/"), repo: repo, token: token}, nil
}

func (g *githubAPI) get(ctx context.Context, path string, target any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.baseURL+path, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+g.token)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := g.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("GitHub API GET %s returned %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 2<<20))
	if err := decoder.Decode(target); err != nil {
		return false, fmt.Errorf("decode GitHub API GET %s: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return false, fmt.Errorf("decode GitHub API GET %s: unexpected trailing data", path)
	}
	return true, nil
}

func (g *githubAPI) openBlocker(ctx context.Context) (*blocker, error) {
	var issues []struct {
		Title   string `json:"title"`
		HTMLURL string `json:"html_url"`
		Number  int    `json:"number"`
	}
	exists, err := g.get(ctx, "/repos/"+g.repo+"/issues?state=open&labels=release-blocker&per_page=1", &issues)
	if err != nil {
		return nil, fmt.Errorf("query release blockers: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("query release blockers: GitHub API returned not found")
	}
	if len(issues) == 0 {
		return nil, nil
	}
	return &blocker{Number: issues[0].Number, Title: issues[0].Title, HTMLURL: issues[0].HTMLURL}, nil
}

func (g *githubAPI) tag(ctx context.Context, name string) (tagInfo, bool, error) {
	var ref struct {
		Object struct{ Type, SHA string } `json:"object"`
	}
	exists, err := g.get(ctx, "/repos/"+g.repo+"/git/ref/tags/"+escapePath(name), &ref)
	if err != nil || !exists {
		return tagInfo{}, exists, err
	}
	if ref.Object.Type != "tag" {
		return tagInfo{Target: ref.Object.SHA}, true, nil
	}
	var annotated struct {
		Object       struct{ Type, SHA string } `json:"object"`
		Verification struct {
			Verified bool `json:"verified"`
		} `json:"verification"`
	}
	exists, err = g.get(ctx, "/repos/"+g.repo+"/git/tags/"+ref.Object.SHA, &annotated)
	if err != nil {
		return tagInfo{}, false, err
	}
	if !exists {
		return tagInfo{}, false, fmt.Errorf("annotated tag object %s disappeared", ref.Object.SHA)
	}
	if annotated.Object.Type != "commit" {
		return tagInfo{}, true, fmt.Errorf("tag %s points to %s object, not a commit", name, annotated.Object.Type)
	}
	return tagInfo{Target: annotated.Object.SHA, Annotated: true, Verified: annotated.Verification.Verified}, true, nil
}

func (g *githubAPI) release(ctx context.Context, tag string) (releaseInfo, bool, error) {
	var result releaseInfo
	exists, err := g.get(ctx, "/repos/"+g.repo+"/releases/tags/"+escapePath(tag), &result)
	return result, exists, err
}
func (g *githubAPI) latestRelease(ctx context.Context) (releaseInfo, bool, error) {
	var result releaseInfo
	exists, err := g.get(ctx, "/repos/"+g.repo+"/releases/latest", &result)
	return result, exists, err
}
func escapePath(value string) string { return strings.ReplaceAll(url.PathEscape(value), "%2F", "/") }

type tagVerifier interface{ verifyRemoteTag(string) error }
type releaseAction string

const (
	actionCreateBoth    releaseAction = "create-tag-and-release"
	actionCreateRelease releaseAction = "create-release"
	actionComplete      releaseAction = "already-complete"
)

func checkBlockers(ctx context.Context, api releaseAPI) error {
	issue, err := api.openBlocker(ctx)
	if err != nil {
		return err
	}
	if issue != nil {
		return fmt.Errorf("release blocked by open release-blocker #%d (%s): %s", issue.Number, issue.Title, issue.HTMLURL)
	}
	return nil
}

func inspectReleaseState(ctx context.Context, api releaseAPI, verifier tagVerifier, c candidate, target string) (releaseAction, error) {
	tag, tagExists, err := api.tag(ctx, c.tag)
	if err != nil {
		return "", fmt.Errorf("query tag %s: %w", c.tag, err)
	}
	release, releaseExists, err := api.release(ctx, c.tag)
	if err != nil {
		return "", fmt.Errorf("query release %s: %w", c.tag, err)
	}
	if !tagExists {
		if releaseExists {
			return "", fmt.Errorf("release %s exists without its tag", c.tag)
		}
		return actionCreateBoth, nil
	}
	if !tag.Annotated {
		return "", fmt.Errorf("tag %s is lightweight; releases require a signed annotated tag", c.tag)
	}
	if !tag.Verified {
		return "", fmt.Errorf("GitHub does not report tag %s as having a verified signature", c.tag)
	}
	if !strings.EqualFold(tag.Target, target) {
		return "", fmt.Errorf("tag %s targets %s, want %s", c.tag, tag.Target, target)
	}
	if err := verifier.verifyRemoteTag(c.tag); err != nil {
		return "", fmt.Errorf("verify signer of tag %s: %w", c.tag, err)
	}
	if !releaseExists {
		return actionCreateRelease, nil
	}
	if release.TagName != c.tag || release.Name != c.title || release.Draft || release.Prerelease != c.prerelease {
		return "", fmt.Errorf("release %s has conflicting metadata (tag=%q title=%q draft=%t prerelease=%t)", c.tag, release.TagName, release.Name, release.Draft, release.Prerelease)
	}
	latest, latestExists, err := api.latestRelease(ctx)
	if err != nil {
		return "", fmt.Errorf("query latest release: %w", err)
	}
	isLatest := latestExists && latest.ID == release.ID
	if c.module == "root" && !c.prerelease {
		if isLatest || (latestExists && isHigherStableRootTag(latest.TagName, c)) {
			return actionComplete, nil
		}
		return "", fmt.Errorf("stable root release %s is not Latest and has not been superseded by a higher stable root release", c.tag)
	}
	if isLatest {
		return "", fmt.Errorf("release %s is Latest, but this module/version must not be", c.tag)
	}
	return actionComplete, nil
}

func isHigherStableRootTag(tag string, c candidate) bool {
	if !strings.HasPrefix(tag, "v") || strings.Contains(tag, "/") {
		return false
	}
	version, err := parseVersion(strings.TrimPrefix(tag, "v"))
	return err == nil && len(version.prerelease) == 0 && compareVersions(version, c.parsedVersion) > 0
}

func verifyNewReleaseLatest(ctx context.Context, api releaseAPI, c candidate) error {
	if c.module != "root" || c.prerelease {
		return nil
	}
	release, exists, err := api.release(ctx, c.tag)
	if err != nil {
		return fmt.Errorf("query newly-created release %s: %w", c.tag, err)
	}
	if !exists {
		return fmt.Errorf("newly-created release %s is missing", c.tag)
	}
	latest, latestExists, err := api.latestRelease(ctx)
	if err != nil {
		return fmt.Errorf("query latest release after creating %s: %w", c.tag, err)
	}
	if !latestExists || latest.ID != release.ID {
		return fmt.Errorf("newly-created stable root release %s is not Latest", c.tag)
	}
	return nil
}
