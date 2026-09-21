package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/mail"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type command struct {
	name  string
	args  []string
	input []byte
	env   []string
}
type commandRunner interface {
	run(context.Context, command) (string, error)
}
type execCommandRunner struct{}

func (execCommandRunner) run(ctx context.Context, spec command) (string, error) {
	cmd := exec.CommandContext(ctx, spec.name, spec.args...)
	cmd.Env = append(os.Environ(), spec.env...)
	if spec.input != nil {
		cmd.Stdin = bytes.NewReader(spec.input)
	}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		return output.String(), fmt.Errorf("%s %s: %w\n%s", spec.name, strings.Join(spec.args, " "), err, strings.TrimSpace(output.String()))
	}
	return output.String(), nil
}

type targetGit interface {
	fetchMaster(context.Context) error
	resolve(context.Context, string) (string, error)
	isAncestor(context.Context, string) (bool, error)
	checkout(context.Context, string) error
}
type gitRepository struct {
	runner commandRunner
	env    []string
}

func (g gitRepository) git(ctx context.Context, args ...string) (string, error) {
	return g.runner.run(ctx, command{name: "git", args: args, env: g.env})
}
func (g gitRepository) fetchMaster(ctx context.Context) error {
	_, err := g.git(ctx, "fetch", "--no-tags", "origin", "+refs/heads/master:refs/remotes/origin/master")
	return err
}
func (g gitRepository) resolve(ctx context.Context, sha string) (string, error) {
	output, err := g.git(ctx, "rev-parse", "--verify", "--end-of-options", sha+"^{commit}")
	return strings.TrimSpace(output), err
}
func (g gitRepository) isAncestor(ctx context.Context, sha string) (bool, error) {
	_, err := g.git(ctx, "merge-base", "--is-ancestor", sha, "refs/remotes/origin/master")
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}
func (g gitRepository) checkout(ctx context.Context, sha string) error {
	_, err := g.git(ctx, "checkout", "--detach", "--force", sha)
	return err
}

func resolveTarget(ctx context.Context, git targetGit, requested string) (string, error) {
	if requested != "master" {
		if err := validateSHA(requested); err != nil {
			return "", fmt.Errorf("target must be master or a full commit SHA: %w", err)
		}
	}
	if err := git.fetchMaster(ctx); err != nil {
		return "", fmt.Errorf("fetch origin/master: %w", err)
	}
	resolveRef := requested
	if requested == "master" {
		resolveRef = "refs/remotes/origin/master"
	}
	resolved, err := git.resolve(ctx, resolveRef)
	if err != nil {
		return "", fmt.Errorf("resolve target commit: %w", err)
	}
	if requested != "master" && !strings.EqualFold(resolved, requested) {
		return "", fmt.Errorf("target resolved to %s, want exact commit %s", resolved, requested)
	}
	ancestor, err := git.isAncestor(ctx, resolved)
	if err != nil {
		return "", fmt.Errorf("check target ancestry: %w", err)
	}
	if !ancestor {
		return "", fmt.Errorf("target commit %s is not reachable from origin/master", resolved)
	}
	if err := git.checkout(ctx, resolved); err != nil {
		return "", fmt.Errorf("checkout target commit: %w", err)
	}
	return strings.ToLower(resolved), nil
}

type trustedKey struct {
	runner                         commandRunner
	home, fingerprint, name, email string
}

func loadTrustedKey(ctx context.Context, runner commandRunner) (*trustedKey, error) {
	fingerprint := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(string(trustedFingerprint)), " ", ""))
	if ok, _ := regexp.MatchString(`^[0-9A-F]{40,64}$`, fingerprint); !ok {
		return nil, fmt.Errorf("committed signing fingerprint is malformed")
	}
	home, err := os.MkdirTemp("", "gocql-release-gnupg-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(home, 0o700); err != nil {
		_ = os.RemoveAll(home)
		return nil, err
	}
	key := &trustedKey{runner: runner, home: home, fingerprint: fingerprint}
	if _, err := key.gpg(ctx, trustedPublicKey, "--import-options", "import-minimal", "--import"); err != nil {
		key.close()
		return nil, fmt.Errorf("import trusted public key: %w", err)
	}
	output, err := key.gpg(ctx, nil, "--with-colons", "--fingerprint", fingerprint)
	if err != nil || !containsFingerprint(output, fingerprint) {
		key.close()
		return nil, fmt.Errorf("committed public key does not contain fingerprint %s", fingerprint)
	}
	key.name, key.email, err = signingIdentity(output)
	if err != nil {
		key.close()
		return nil, err
	}
	return key, nil
}
func (k *trustedKey) close() { _ = os.RemoveAll(k.home) }
func (k *trustedKey) gpg(ctx context.Context, input []byte, args ...string) (string, error) {
	return k.runner.run(ctx, command{name: "gpg", args: append([]string{"--batch", "--homedir", k.home}, args...), input: input})
}
func containsFingerprint(output, fingerprint string) bool {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Split(line, ":")
		if len(fields) > 9 && fields[0] == "fpr" && strings.EqualFold(fields[9], fingerprint) {
			return true
		}
	}
	return false
}
func signingIdentity(output string) (string, string, error) {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Split(line, ":")
		if len(fields) <= 9 || fields[0] != "uid" {
			continue
		}
		uid, err := decodeGPGField(fields[9])
		if err != nil {
			return "", "", fmt.Errorf("decode signing-key identity: %w", err)
		}
		address, err := mail.ParseAddress(uid)
		if err != nil || address.Name == "" || address.Address == "" {
			return "", "", fmt.Errorf("trusted signing key has unusable primary identity %q", uid)
		}
		return address.Name, address.Address, nil
	}
	return "", "", fmt.Errorf("trusted signing key has no identity")
}
func decodeGPGField(value string) (string, error) {
	var decoded strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' {
			decoded.WriteByte(value[i])
			continue
		}
		if i+3 >= len(value) || value[i+1] != 'x' {
			return "", fmt.Errorf("unsupported escape in %q", value)
		}
		byteValue, err := strconv.ParseUint(value[i+2:i+4], 16, 8)
		if err != nil {
			return "", fmt.Errorf("invalid escape in %q", value)
		}
		decoded.WriteByte(byte(byteValue))
		i += 3
	}
	return decoded.String(), nil
}

type gitTagVerifier struct {
	key  *trustedKey
	repo gitRepository
}

func (v gitTagVerifier) verifyRemoteTag(tag string) error {
	ctx := context.Background()
	ref := "refs/release-validation/" + tag
	if _, err := v.repo.git(ctx, "fetch", "--no-tags", "--force", "origin", "+refs/tags/"+tag+":"+ref); err != nil {
		return err
	}
	output, err := v.repo.runner.run(ctx, command{name: "git", args: []string{"verify-tag", "--raw", ref}, env: []string{"GNUPGHOME=" + v.key.home}})
	if err != nil {
		return err
	}
	if !containsValidSignature(output, v.key.fingerprint) {
		return fmt.Errorf("valid signature was not made by trusted key %s", v.key.fingerprint)
	}
	return nil
}
func (v gitTagVerifier) verifyLocalTag(ctx context.Context, tag string) error {
	output, err := v.repo.runner.run(ctx, command{name: "git", args: []string{"verify-tag", "--raw", tag}, env: []string{"GNUPGHOME=" + v.key.home}})
	if err != nil {
		return err
	}
	if !containsValidSignature(output, v.key.fingerprint) {
		return fmt.Errorf("tag was not signed by trusted key %s", v.key.fingerprint)
	}
	return nil
}
func containsValidSignature(output, fingerprint string) bool {
	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, "[GNUPG:] VALIDSIG ") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if strings.EqualFold(field, fingerprint) {
				return true
			}
		}
	}
	return false
}
func (k *trustedKey) importPrivate(ctx context.Context, private []byte) error {
	if len(private) == 0 {
		return fmt.Errorf("GPG_PRIVATE_KEY is empty")
	}
	if _, err := k.gpg(ctx, private, "--import"); err != nil {
		return fmt.Errorf("import private signing key: %w", err)
	}
	output, err := k.gpg(ctx, nil, "--with-colons", "--fingerprint", "--list-secret-keys", k.fingerprint)
	if err != nil || !containsFingerprint(output, k.fingerprint) {
		return fmt.Errorf("private key does not match committed fingerprint %s", k.fingerprint)
	}
	return nil
}

func previousTag(ctx context.Context, repo gitRepository, c candidate, target string) (string, error) {
	pattern := "v*"
	if c.module == "lz4" {
		pattern = "lz4/v*"
	}
	output, err := repo.git(ctx, "tag", "--list", pattern, "--merged", target)
	if err != nil {
		return "", err
	}
	type versionTag struct {
		name    string
		version semVersion
	}
	var candidates []versionTag
	for _, tag := range strings.Fields(output) {
		bare := strings.TrimPrefix(tag, "lz4/")
		if !strings.HasPrefix(bare, "v") {
			continue
		}
		parsed, err := parseVersion(strings.TrimPrefix(bare, "v"))
		if err != nil || compareVersions(parsed, c.parsedVersion) >= 0 {
			continue
		}
		candidates = append(candidates, versionTag{name: tag, version: parsed})
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no preceding %s release tag is reachable from %s", c.module, target)
	}
	sort.Slice(candidates, func(i, j int) bool { return compareVersions(candidates[i].version, candidates[j].version) > 0 })
	return candidates[0].name, nil
}
func writeGPGWrapper(directory string) (string, error) {
	path := filepath.Join(directory, "gpg-wrapper")
	content := "#!/bin/sh\nexec gpg --batch --yes --pinentry-mode loopback --passphrase-file \"$GPG_PASSPHRASE_FILE\" \"$@\"\n"
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		return "", err
	}
	return path, nil
}
