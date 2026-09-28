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

# CCM installs a partial release under the resolved X.Y.Z directory but looks
# for the partial name again on its next operation, missing that installation
# and querying S3 each time. Release references must therefore name one build.
# Unstable and hotfix references are intentionally preserved: CCM revalidates
# those mutable builds by hash even when their installation directory exists.
# CCM normalizes the three spelling variants of numbered release candidates;
# alpha/beta spellings follow the same exact, numbered-release convention.
readonly exact_release_re='^([0-9]+\.[0-9]+\.[0-9]+|[0-9]+\.[0-9]+(\.[0-9]+)?[-.~](alpha|beta|rc)[0-9]+)$'

is_mutable_ccm_version() {
	local version=$1
	local reference
	local type_and_branch
	local branch
	local build

	[[ ! "${version}" =~ [[:space:]] ]] || return 1
	# CCM removes the terminal mode suffix before it parses type/branch:build.
	# Validate that remaining reference so ':debug' cannot masquerade as a build.
	reference=${version%:debug}
	[[ "${reference}" == *:* ]] || return 1
	type_and_branch=${reference%%:*}
	branch=${type_and_branch#*/}
	build=${reference#*:}
	build=${build%%:*}
	[[ -n "${branch}" && -n "${build}" ]]
}

is_exact_ccm_version() {
	local version=$1
	local release

	case "${version}" in
	unstable/* | hotfix/*)
		is_mutable_ccm_version "${version}"
		return
		;;
	release:*)
		release=${version#release:}
		release=${release%:debug}
		[[ "${release}" =~ ${exact_release_re} ]]
		return
		;;
	*)
		return 1
		;;
	esac
}

lookup_release() {
	local repository=$1
	local filter=$2
	local get_version=${GET_VERSION_BIN:-get-version}

	"${get_version}" \
		--source dockerhub-imagetag \
		--repo "${repository}" \
		--filters "${filter}" | tr -d '"\r'
}

emit_release() {
	local release=$1
	local debug_suffix=${2:-}
	local ccm_version=release:${release}${debug_suffix}

	if ! is_exact_ccm_version "${ccm_version}"; then
		printf "Resolved ScyllaDB version '%s' does not name one release build\n" "${release}" >&2
		return 1
	fi

	printf '%s\n' "${ccm_version}"
}

resolve_release() {
	local requested=$1
	local resolved=

	case "${requested}" in
	LTS-LATEST)
		resolved=$(lookup_release scylladb/scylla '^[0-9]{4}$.^[0-9]+$.^[0-9]+$ and LAST.1.LAST') || return
		;;
	LTS-PRIOR)
		resolved=$(lookup_release scylladb/scylla '^[0-9]{4}$.^[0-9]+$.^[0-9]+$ and LAST-1.1.LAST') || return
		if [[ -z "${resolved}" ]]; then
			resolved=$(lookup_release scylladb/scylla-enterprise '^[0-9]{4}$.^[0-9]+$.^[0-9]+$ and LAST-1.1.LAST') || return
		fi
		;;
	LATEST)
		resolved=$(lookup_release scylladb/scylla '^[0-9]{4}$.^[0-9]+$.^[0-9]+$ and LAST.LAST.LAST') || return
		;;
	PRIOR)
		resolved=$(lookup_release scylladb/scylla '^[0-9]{4}$.^[0-9]+$.^[0-9]+$ and LAST.LAST.LAST-1') || return
		;;
	*)
		return 1
		;;
	esac

	printf '%s' "${resolved}"
}

resolve_version() {
	local requested=$1
	local release=${requested}
	local debug_suffix=
	local resolved=

	case "${requested}" in
	unstable/* | hotfix/*)
		if ! is_mutable_ccm_version "${requested}"; then
			printf "Invalid ScyllaDB mutable build reference '%s'\n" "${requested}" >&2
			return 1
		fi
		printf '%s\n' "${requested}"
		return
		;;
	LTS-LATEST | LTS-PRIOR | LATEST | PRIOR)
		resolved=$(resolve_release "${requested}") || return
		if [[ -n "${resolved}" ]]; then
			emit_release "${resolved}" || return
		fi
		return
		;;
	release:*)
		release=${requested#release:}
		;;
	esac

	if [[ "${release}" == *:debug ]]; then
		release=${release%:debug}
		debug_suffix=:debug
	fi

	if [[ "${release}" =~ ${exact_release_re} ]]; then
		emit_release "${release}" "${debug_suffix}" || return
		return
	fi

	if [[ "${release}" =~ ^[0-9]+\.[0-9]+$ ]]; then
		resolved=$(lookup_release scylladb/scylla "^[0-9]+$.^[0-9]+$.^[0-9]+$ and ${release}.LAST") || return
		# Four-digit release lines before 2025.1 were published in the
		# scylla-enterprise repository. OSS lines need no second lookup.
		if [[ -z "${resolved}" && "${release}" =~ ^[0-9]{4}\. ]]; then
			resolved=$(lookup_release scylladb/scylla-enterprise "^[0-9]+$.^[0-9]+$.^[0-9]+$ and ${release}.LAST") || return
		fi
		if [[ -n "${resolved}" ]]; then
			emit_release "${resolved}" "${debug_suffix}" || return
		fi
		return
	fi

	printf "Unknown ScyllaDB version '%s'\n" "${requested}" >&2
	printf '%s\n' 'Expected an alias, release:MAJOR.MINOR.PATCH, MAJOR.MINOR.PATCH, MAJOR.MINOR, or an unstable/hotfix reference' >&2
	return 1
}

main() {
	if [[ ${1:-} == --check ]]; then
		[[ $# == 2 ]] || return 2
		is_exact_ccm_version "$2"
		return
	fi

	if [[ $# != 1 ]]; then
		printf 'usage: %s [--check] VERSION\n' "$0" >&2
		return 2
	fi

	resolve_version "$1"
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
	main "$@"
fi
