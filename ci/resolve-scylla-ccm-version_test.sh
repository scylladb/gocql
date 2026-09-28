#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

readonly repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
readonly resolver=${repo_root}/ci/resolve-scylla-ccm-version.sh
readonly test_dir=$(mktemp -d)
readonly lookup_log=${test_dir}/lookups
readonly fake_get_version=${test_dir}/get-version
trap 'rm -rf "${test_dir}"' EXIT

cat >"${fake_get_version}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} == -version ]]; then
	printf '0.4.5\n'
	exit
fi

repo=
filter=
while (($#)); do
	case "$1" in
	--repo)
		repo=$2
		shift 2
		;;
	--filters)
		filter=$2
		shift 2
		;;
	*)
		shift
		;;
	esac
done

printf '%s|%s\n' "${repo}" "${filter}" >>"${LOOKUP_LOG}"
case "${repo}|${filter}" in
*" and 2026.2.LAST") printf '2026.2.7\n' ;;
"scylladb/scylla-enterprise|"*" and 2024.2.LAST") printf '2024.2.13\n' ;;
*" and LAST.1.LAST") printf '2026.1.9\n' ;;
*" and LAST-1.1.LAST") printf '2025.1.12\n' ;;
*" and LAST.LAST.LAST-1") printf '2026.2.6\n' ;;
*" and LAST.LAST.LAST") printf '2026.2.7\n' ;;
*" and 2026.9.LAST") exit 23 ;;
*" and 2040.1.LAST") printf '2040.1\n' ;;
esac
EOF
chmod +x "${fake_get_version}"

fail() {
	printf 'FAIL: %s\n' "$*" >&2
	exit 1
}

resolve() {
	LOOKUP_LOG=${lookup_log} GET_VERSION_BIN=${fake_get_version} bash "${resolver}" "$1"
}

assert_resolves() {
	local requested=$1
	local expected=$2
	local actual

	: >"${lookup_log}"
	actual=$(resolve "${requested}")
	[[ "${actual}" == "${expected}" ]] || fail "${requested} resolved to '${actual}', want '${expected}'"
}

assert_no_lookup() {
	[[ ! -s "${lookup_log}" ]] || fail "unexpected lookup: $(<"${lookup_log}")"
}

assert_rejected() {
	local requested=$1

	: >"${lookup_log}"
	if resolve "${requested}" >/dev/null 2>&1; then
		fail "${requested} was accepted"
	fi
}

assert_resolves 2026.2 release:2026.2.7
assert_resolves release:2026.2 release:2026.2.7
assert_resolves release:2026.2:debug release:2026.2.7:debug
assert_resolves 2024.2 release:2024.2.13
[[ $(wc -l <"${lookup_log}") == 2 ]] || fail 'enterprise release did not use the two-repository fallback'

assert_resolves LATEST release:2026.2.7
assert_resolves PRIOR release:2026.2.6
assert_resolves LTS-LATEST release:2026.1.9
assert_resolves LTS-PRIOR release:2025.1.12

assert_resolves 2026.2.2 release:2026.2.2
assert_no_lookup
assert_resolves release:2026.2.2:debug release:2026.2.2:debug
assert_no_lookup
assert_resolves release:2026.2.0~rc1 release:2026.2.0~rc1
assert_no_lookup
assert_resolves release:4.0-alpha1 release:4.0-alpha1
assert_no_lookup
assert_resolves release:4.0.beta2 release:4.0.beta2
assert_no_lookup
assert_resolves unstable/master:latest unstable/master:latest
assert_no_lookup
assert_resolves unstable/master:latest:debug unstable/master:latest:debug
assert_no_lookup
assert_resolves unstable/branch-6.2:123:debug unstable/branch-6.2:123:debug
assert_no_lookup
assert_resolves unstable/master:2026-09-28T12:34:56Z unstable/master:2026-09-28T12:34:56Z
assert_no_lookup
assert_resolves hotfix/branch-6.2:123 hotfix/branch-6.2:123
assert_no_lookup
assert_resolves hotfix/branch-6.2:123:debug hotfix/branch-6.2:123:debug
assert_no_lookup
assert_resolves hotfix/branch-6.2:2026-09-28T12:34:56Z:debug hotfix/branch-6.2:2026-09-28T12:34:56Z:debug
assert_no_lookup
assert_resolves hotfix/branch-6.2:latest hotfix/branch-6.2:latest
assert_no_lookup

assert_rejected release:2026
assert_rejected release:2026.2.0-dev
assert_rejected release:2022.1.3-dev-0.20220922.539a55e35
assert_rejected release:2026.2.0~rc
assert_rejected unknown
assert_rejected 2040.1
assert_rejected unstable/master
assert_rejected unstable/
assert_rejected unstable/:123
assert_rejected unstable/master:
assert_rejected unstable/master:debug
assert_rejected unstable/master::debug
assert_rejected unstable/master:123:extra
assert_rejected unstable/master:123:extra:debug
assert_rejected unstable/master:2026-09-28T12:34:56
assert_rejected unstable/master:2026-9-28T12:34:56Z
assert_rejected unstable/master:2026-09-28T12:34:56Z:extra
assert_rejected hotfix/
assert_rejected hotfix/branch
assert_rejected hotfix/branch:debug
assert_rejected hotfix/branch:123:extra
assert_rejected hotfix/branch:2026-09-28T12:34:56Z:extra

: >"${lookup_log}"
[[ -z $(resolve 2030.9) ]] || fail 'missing release produced output'
if resolve 2026.9 >/dev/null 2>&1; then
	fail 'lookup failure was swallowed'
else
	status=$?
	[[ ${status} == 23 ]] || fail "lookup failed with ${status}, want 23"
fi

# Do not rely on errexit for lookup propagation. Bash disables it inside a
# function when that function is evaluated as an if-condition.
source "${resolver}"
if LOOKUP_LOG=${lookup_log} GET_VERSION_BIN=${fake_get_version} resolve_version 2026.9 >/dev/null 2>&1; then
	fail 'lookup failure was swallowed in a conditional function call'
else
	status=$?
	[[ ${status} == 23 ]] || fail "conditional lookup failed with ${status}, want 23"
fi

# Exercise the Make boundary too: old cache contents are rejected and a lookup
# error aborts the target instead of becoming an empty successful resolution.
make_cache=${test_dir}/make-cache
printf 'release:2026.2\n' >"${make_cache}"
GITHUB_ENV= GITHUB_OUTPUT= LOOKUP_LOG=${lookup_log} make --no-print-directory -s -C "${repo_root}" \
	resolve-scylla-version \
	GET_VERSION_BIN="${fake_get_version}" \
	SCYLLA_VERSION=2026.2 \
	SCYLLA_VERSION_FILE="${make_cache}" >/dev/null
[[ $(<"${make_cache}") == release:2026.2.7 ]] || fail 'Make boundary kept a partial cached release'

cache_key_a=$(make --no-print-directory -s -C "${repo_root}" \
	--eval 'print-scylla-cache-key: ; @echo "$(SCYLLA_VERSION_CACHE_KEY)"' \
	print-scylla-cache-key 'SCYLLA_VERSION=unstable/a_b:c')
cache_key_b=$(make --no-print-directory -s -C "${repo_root}" \
	--eval 'print-scylla-cache-key: ; @echo "$(SCYLLA_VERSION_CACHE_KEY)"' \
	print-scylla-cache-key 'SCYLLA_VERSION=unstable/a:b_c')
[[ "${cache_key_a}" != "${cache_key_b}" ]] || fail 'mutable references collide in the Make cache key'

if GITHUB_ENV= GITHUB_OUTPUT= LOOKUP_LOG=${lookup_log} make --no-print-directory -s -C "${repo_root}" \
	resolve-scylla-version \
	GET_VERSION_BIN="${fake_get_version}" \
	SCYLLA_VERSION=2026.9 \
	SCYLLA_VERSION_FILE="${test_dir}/failed-cache" >/dev/null 2>&1; then
	fail 'Make boundary swallowed a lookup failure'
fi

bash "${resolver}" --check release:2026.2.2
bash "${resolver}" --check release:2026.2.0~rc1:debug
bash "${resolver}" --check unstable/master:latest
for invalid in release:2026.2 2026.2.2 unstable/master unstable/:123 unstable/master: unstable/master:debug unstable/master:123:extra unstable/master:2026-09-28T12:34:56 hotfix/branch:debug hotfix/branch:123:extra; do
	if bash "${resolver}" --check "${invalid}"; then
		fail "check accepted invalid CCM reference ${invalid}"
	fi
done

printf 'ScyllaDB CCM version resolver tests passed\n'
