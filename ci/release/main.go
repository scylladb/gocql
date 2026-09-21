package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type environment struct{ module, version, target, dispatchRef, repository, apiURL, apiToken, blockerToken, output string }

func main() {
	if err := run(context.Background(), os.Args[1:], execCommandRunner{}); err != nil {
		fmt.Fprintf(os.Stderr, "release validation failed: %v\n", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, runner commandRunner) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: go run ./ci/release <preflight|gate|publish>")
	}
	env := environment{
		module: os.Getenv("RELEASE_MODULE"), version: os.Getenv("RELEASE_VERSION"), target: os.Getenv("RELEASE_TARGET_COMMIT"),
		dispatchRef: os.Getenv("RELEASE_DISPATCH_REF"), repository: os.Getenv("GITHUB_REPOSITORY"), apiURL: os.Getenv("GITHUB_API_URL"),
		apiToken: os.Getenv("GH_TOKEN"), blockerToken: os.Getenv("RELEASE_QUERY_TOKEN"), output: os.Getenv("GITHUB_OUTPUT"),
	}
	c, err := newCandidate(env.module, env.version)
	if err != nil {
		return err
	}
	if err := validateSHA(env.target); err != nil {
		return err
	}
	switch args[0] {
	case "preflight":
		return preflight(ctx, runner, env, c)
	case "gate":
		return gate(ctx, runner, env, c)
	case "publish":
		return publish(ctx, runner, env, c)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func preflight(ctx context.Context, runner commandRunner, env environment, c candidate) error {
	if env.dispatchRef != "refs/heads/master" {
		return fmt.Errorf("release workflow must be dispatched from master, got %q", env.dispatchRef)
	}
	api, err := newGitHubAPI(env.apiURL, env.repository, env.apiToken)
	if err != nil {
		return err
	}
	if err := checkBlockers(ctx, api); err != nil {
		return err
	}
	repo := gitRepository{runner: runner}
	resolved, err := resolveTarget(ctx, repo, env.target)
	if err != nil {
		return err
	}
	if err := validateModuleFile(c); err != nil {
		return err
	}
	if err := validateRootREADME(c); err != nil {
		return err
	}
	goArgs := []string{}
	if c.directory != "." {
		goArgs = append(goArgs, "-C", c.directory)
	}
	if _, err := runner.run(ctx, command{name: "go", args: append(append([]string{}, goArgs...), "mod", "tidy", "-diff")}); err != nil {
		return fmt.Errorf("module metadata is not tidy: %w", err)
	}
	if _, err := runner.run(ctx, command{name: "go", args: append(append([]string{}, goArgs...), "mod", "verify")}); err != nil {
		return fmt.Errorf("module verification failed: %w", err)
	}
	key, err := loadTrustedKey(ctx, runner)
	if err != nil {
		return err
	}
	defer key.close()
	action, err := inspectReleaseState(ctx, api, gitTagVerifier{repo: repo, key: key}, c, resolved)
	if err != nil {
		return err
	}
	if err := appendOutputs(env.output, map[string]string{"directory": c.directory, "module_path": c.modulePath, "release_action": string(action), "release_title": c.title, "resolved_sha": resolved, "tag": c.tag}); err != nil {
		return err
	}
	fmt.Printf("preflight passed for %s at %s (%s)\n", c.tag, resolved, action)
	return nil
}

func gate(ctx context.Context, runner commandRunner, env environment, c candidate) error {
	api, err := newGitHubAPI(env.apiURL, env.repository, env.apiToken)
	if err != nil {
		return err
	}
	if err := checkBlockers(ctx, api); err != nil {
		return err
	}
	key, err := loadTrustedKey(ctx, runner)
	if err != nil {
		return err
	}
	defer key.close()
	action, err := inspectReleaseState(ctx, api, gitTagVerifier{repo: gitRepository{runner: runner}, key: key}, c, strings.ToLower(env.target))
	if err != nil {
		return err
	}
	if err := appendOutputs(env.output, map[string]string{"release_action": string(action)}); err != nil {
		return err
	}
	fmt.Printf("publication gate passed for %s (%s)\n", c.tag, action)
	return nil
}

func publish(ctx context.Context, runner commandRunner, env environment, c candidate) error {
	if env.blockerToken == "" {
		return fmt.Errorf("RELEASE_QUERY_TOKEN is required for final blocker check")
	}
	queryAPI, err := newGitHubAPI(env.apiURL, env.repository, env.blockerToken)
	if err != nil {
		return err
	}
	if err := checkBlockers(ctx, queryAPI); err != nil {
		return err
	}
	api, err := newGitHubAPI(env.apiURL, env.repository, env.apiToken)
	if err != nil {
		return err
	}
	key, err := loadTrustedKey(ctx, runner)
	if err != nil {
		return err
	}
	defer key.close()
	repo := gitRepository{runner: runner}
	verifier := gitTagVerifier{repo: repo, key: key}
	action, err := inspectReleaseState(ctx, api, verifier, c, strings.ToLower(env.target))
	if err != nil {
		return err
	}
	if action == actionComplete {
		fmt.Printf("%s and its GitHub Release already exist and are valid\n", c.tag)
		return nil
	}
	if action == actionCreateBoth {
		if err := key.importPrivate(ctx, []byte(os.Getenv("GPG_PRIVATE_KEY"))); err != nil {
			return err
		}
		passphrase := os.Getenv("GPG_PASSPHRASE")
		if passphrase == "" {
			return fmt.Errorf("GPG_PASSPHRASE is empty")
		}
		secretDir, err := os.MkdirTemp("", "gocql-release-sign-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(secretDir)
		if err := os.Chmod(secretDir, 0o700); err != nil {
			return err
		}
		passphraseFile := filepath.Join(secretDir, "passphrase")
		if err := os.WriteFile(passphraseFile, []byte(passphrase), 0o600); err != nil {
			return err
		}
		wrapper, err := writeGPGWrapper(secretDir)
		if err != nil {
			return err
		}
		signingEnv := []string{"GNUPGHOME=" + key.home, "GPG_PASSPHRASE_FILE=" + passphraseFile}
		_, err = runner.run(ctx, command{name: "git", env: signingEnv, args: []string{
			"-c", "user.name=" + key.name, "-c", "user.email=" + key.email, "-c", "user.signingkey=" + key.fingerprint,
			"-c", "gpg.program=" + wrapper, "tag", "--sign", "--annotate", c.tag, env.target, "--message", c.title,
		}})
		if err != nil {
			return fmt.Errorf("create signed tag: %w", err)
		}
		if err := verifier.verifyLocalTag(ctx, c.tag); err != nil {
			return fmt.Errorf("verify newly-created tag: %w", err)
		}
		if _, err := repo.git(ctx, "push", "origin", "refs/tags/"+c.tag+":refs/tags/"+c.tag); err != nil {
			return fmt.Errorf("push tag: %w", err)
		}
	}
	previous, err := previousTag(ctx, repo, c, env.target)
	if err != nil {
		return err
	}
	args := []string{"release", "create", c.tag, "--repo", env.repository, "--verify-tag", "--generate-notes", "--notes-start-tag", previous, "--target", env.target, "--title", c.title}
	if c.prerelease {
		args = append(args, "--prerelease")
	}
	if c.module == "root" && !c.prerelease {
		args = append(args, "--latest")
	} else {
		args = append(args, "--latest=false")
	}
	if _, err := runner.run(ctx, command{name: "gh", args: args}); err != nil {
		return fmt.Errorf("create GitHub Release: %w", err)
	}
	var finalAction releaseAction
	for attempt := 0; attempt < 5; attempt++ {
		finalAction, err = inspectReleaseState(ctx, api, verifier, c, strings.ToLower(env.target))
		if err == nil && finalAction == actionComplete {
			fmt.Printf("published and verified %s from %s\n", c.tag, env.target)
			return nil
		}
		if attempt != 4 {
			time.Sleep(2 * time.Second)
		}
	}
	if err != nil {
		return fmt.Errorf("verify published release: %w", err)
	}
	return fmt.Errorf("publication did not converge to complete state (got %s)", finalAction)
}

func appendOutputs(path string, values map[string]string) error {
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open GITHUB_OUTPUT: %w", err)
	}
	defer f.Close()
	for key, value := range values {
		if strings.ContainsAny(key+value, "\r\n") {
			return fmt.Errorf("refusing to write multiline workflow output %q", key)
		}
		if _, err := fmt.Fprintf(f, "%s=%s\n", key, value); err != nil {
			return err
		}
	}
	return nil
}
