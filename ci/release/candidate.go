// Copyright (C) 2026 ScyllaDB
// Use of this source code is governed by a license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	rootModulePath = "github.com/gocql/gocql"
	lz4ModulePath  = "github.com/scylladb/gocql/lz4"
)

var (
	hexSHA            = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	identifier        = regexp.MustCompile(`^[0-9A-Za-z-]+$`)
	rootReplacementRE = regexp.MustCompile(`^\s*replace\s+github\.com/gocql/gocql\s+=>\s+github\.com/scylladb/gocql\s+(v[^\s]+)\s*$`)
)

type semVersion struct {
	prerelease          []string
	major, minor, patch int
}

type candidate struct {
	module, modulePath, directory string
	version, tag, title           string
	parsedVersion                 semVersion
	prerelease                    bool
}

func newCandidate(module, version string) (candidate, error) {
	parsed, err := parseVersion(version)
	if err != nil {
		return candidate{}, err
	}
	c := candidate{module: module, version: version, prerelease: len(parsed.prerelease) != 0, parsedVersion: parsed}
	switch module {
	case "root":
		c.modulePath, c.directory, c.tag, c.title = rootModulePath, ".", "v"+version, "v"+version
	case "lz4":
		c.modulePath, c.directory, c.tag, c.title = lz4ModulePath, "lz4", "lz4/v"+version, "lz4 v"+version
	default:
		return candidate{}, fmt.Errorf("module must be root or lz4, got %q", module)
	}
	return c, nil
}

func parseVersion(value string) (semVersion, error) {
	if value == "" {
		return semVersion{}, fmt.Errorf("version is required")
	}
	if strings.HasPrefix(value, "v") {
		return semVersion{}, fmt.Errorf("version must be bare (without a leading v): %q", value)
	}
	if strings.Contains(value, "+") {
		return semVersion{}, fmt.Errorf("build metadata is not allowed in release versions: %q", value)
	}
	core, pre, hasPre := strings.Cut(value, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return semVersion{}, fmt.Errorf("version must contain major, minor, and patch components: %q", value)
	}
	numbers := make([]int, 3)
	for i, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') || !numericIdentifier(part) {
			return semVersion{}, fmt.Errorf("numeric version components must be canonical: %q", value)
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return semVersion{}, fmt.Errorf("parse version %q: %w", value, err)
		}
		numbers[i] = n
	}
	if numbers[0] != 1 {
		return semVersion{}, fmt.Errorf("only v1 modules can be released, got %q", value)
	}
	parsed := semVersion{major: numbers[0], minor: numbers[1], patch: numbers[2]}
	if !hasPre {
		return parsed, nil
	}
	if pre == "" {
		return semVersion{}, fmt.Errorf("prerelease cannot be empty: %q", value)
	}
	for _, part := range strings.Split(pre, ".") {
		if !identifier.MatchString(part) {
			return semVersion{}, fmt.Errorf("invalid prerelease identifier in %q", value)
		}
		if numericIdentifier(part) && len(part) > 1 && part[0] == '0' {
			return semVersion{}, fmt.Errorf("numeric prerelease identifiers must not have leading zeroes: %q", value)
		}
		parsed.prerelease = append(parsed.prerelease, part)
	}
	return parsed, nil
}

func compareVersions(a, b semVersion) int {
	for _, pair := range [][2]int{{a.major, b.major}, {a.minor, b.minor}, {a.patch, b.patch}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	if len(a.prerelease) == 0 && len(b.prerelease) == 0 {
		return 0
	}
	if len(a.prerelease) == 0 {
		return 1
	}
	if len(b.prerelease) == 0 {
		return -1
	}
	for i := 0; i < len(a.prerelease) && i < len(b.prerelease); i++ {
		x, y := a.prerelease[i], b.prerelease[i]
		if x == y {
			continue
		}
		xn, yn := numericIdentifier(x), numericIdentifier(y)
		switch {
		case xn && yn:
			if len(x) < len(y) || (len(x) == len(y) && x < y) {
				return -1
			}
			return 1
		case xn:
			return -1
		case yn:
			return 1
		case x < y:
			return -1
		default:
			return 1
		}
	}
	if len(a.prerelease) < len(b.prerelease) {
		return -1
	}
	if len(a.prerelease) > len(b.prerelease) {
		return 1
	}
	return 0
}

func numericIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func validateModuleFile(c candidate) error {
	path := "go.mod"
	if c.directory != "." {
		path = c.directory + "/go.mod"
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	var got string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "module" {
			got = fields[1]
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if got != c.modulePath {
		return fmt.Errorf("%s module path is %q, want %q", path, got, c.modulePath)
	}
	return nil
}

func validateRootREADME(c candidate) error {
	if c.module != "root" {
		return nil
	}
	data, err := os.ReadFile("README.md")
	if err != nil {
		return fmt.Errorf("read README.md: %w", err)
	}
	want := "v" + c.version
	var documented []string
	for _, line := range strings.Split(string(data), "\n") {
		if match := rootReplacementRE.FindStringSubmatch(line); len(match) == 2 {
			documented = append(documented, match[1])
		}
	}
	if len(documented) == 0 {
		return fmt.Errorf("README.md has no concrete root replacement version; want %s", want)
	}
	for _, got := range documented {
		if got != want {
			return fmt.Errorf("README.md documents root replacement %s, want %s", got, want)
		}
	}
	return nil
}

func validateSHA(value string) error {
	if !hexSHA.MatchString(value) {
		return fmt.Errorf("target_commit must be a full 40-character hexadecimal commit SHA, got %q", value)
	}
	return nil
}
