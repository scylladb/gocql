SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
KEY_PATH = ${MAKEFILE_PATH}/testdata/pki
BIN_DIR := "${MAKEFILE_PATH}/bin"

CASSANDRA_VERSION ?= LATEST
SCYLLA_VERSION ?= LATEST

GOLANGCI_VERSION = 2.5.0
GET_VERSION_VERSION = 0.4.5
GET_VERSION_BIN = $(MAKEFILE_PATH)/bin/get-version

TEST_CQL_PROTOCOL ?= 4
TEST_COMPRESSOR ?= snappy
TEST_OPTS ?=
TEST_INTEGRATION_TAGS ?= integration gocql_debug
JVM_EXTRA_OPTS ?= -Dcassandra.test.fail_writes_ks=test -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler

# COVER_ARGS is appended to every `go test ... ./...` invocation below. It is
# empty by default, so plain `make test-unit` behaves exactly as before; the
# *-coverage targets further down set it via a recursive `make` call to
# instrument the same test run instead of duplicating its recipe.
COVER_ARGS ?=

# test-integration-scylla/cassandra can't use the same COVER_ARGS trick: they
# already pass custom, test-binary-defined flags (-distribution, -cluster,
# ...) that `go test` itself doesn't recognize. Once `go test` hits the first
# such flag, it stops parsing its own flags -- including the package pattern
# (./...), which then silently defaults to "." instead of erroring -- so
# anything placed after it (like COVER_ARGS previously was) is unreliable.
# COVER_BUILD_ARGS (flags `go test` itself must recognize, e.g. -cover)
# goes before the package pattern; COVER_RUNTIME_ARGS (everything meant for
# the test binary, after -args) goes after it, alongside the custom flags.
COVER_BUILD_ARGS ?=
COVER_RUNTIME_ARGS ?=
COVERAGE_DIR ?= $(MAKEFILE_PATH)/.coverage/data

CCM_CASSANDRA_CLUSTER_NAME = gocql_cassandra_integration_test
CCM_CASSANDRA_IP_PREFIX = 127.0.1.
CCM_CASSANDRA_REPO ?= github.com/apache/cassandra-ccm
CCM_CASSANDRA_VERSION ?= trunk

CCM_SCYLLA_CLUSTER_NAME = gocql_scylla_integration_test
CCM_SCYLLA_IP_PREFIX = 127.0.2.
CCM_SCYLLA_REPO ?= github.com/scylladb/scylla-ccm
CCM_SCYLLA_VERSION ?= master

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

CASSANDRA_CONFIG ?= "client_encryption_options.enabled: true" \
"client_encryption_options.keystore: ${KEY_PATH}/.keystore" \
"client_encryption_options.keystore_password: cassandra" \
"client_encryption_options.require_client_auth: true" \
"client_encryption_options.truststore: ${KEY_PATH}/.truststore" \
"client_encryption_options.truststore_password: cassandra" \
"concurrent_reads: 2" \
"concurrent_writes: 2" \
"write_request_timeout_in_ms: 5000" \
"read_request_timeout_in_ms: 5000"

ifeq (${CASSANDRA_VERSION},3-LATEST)
	CASSANDRA_CONFIG += "rpc_server_type: sync" \
"rpc_min_threads: 2" \
"rpc_max_threads: 2" \
"enable_user_defined_functions: true" \
"enable_materialized_views: true" \

else ifeq (${CASSANDRA_VERSION},4-LATEST)
	CASSANDRA_CONFIG +=	"enable_user_defined_functions: true" \
"enable_materialized_views: true"
else
	CASSANDRA_CONFIG += "user_defined_functions_enabled: true" \
"materialized_views_enabled: true"
endif

SCYLLA_CONFIG = "native_transport_port_ssl: 9142" \
"native_transport_port: 9042" \
"native_shard_aware_transport_port: 19042" \
"native_shard_aware_transport_port_ssl: 19142" \
"client_encryption_options.enabled: true" \
"client_encryption_options.certificate: ${KEY_PATH}/cassandra.crt" \
"client_encryption_options.keyfile: ${KEY_PATH}/cassandra.key" \
"client_encryption_options.truststore: ${KEY_PATH}/ca.crt" \
"client_encryption_options.require_client_auth: true" \
"maintenance_socket: workdir" \
"enable_tablets: true" \
"enable_user_defined_functions: true" \
"experimental_features: [udf]"

export JVM_EXTRA_OPTS
export JAVA11_HOME=${JAVA_HOME_11_X64}
export JAVA17_HOME=${JAVA_HOME_17_X64}
export JAVA_HOME=${JAVA_HOME_11_X64}
export PATH := $(MAKEFILE_PATH)/bin:~/.sdkman/bin:$(PATH)

print-config:
	echo ${CASSANDRA_CONFIG}

.prepare-bin:
	@[[ -d "$(MAKEFILE_PATH)/bin" ]] || mkdir "$(MAKEFILE_PATH)/bin"

.prepare-get-version: .prepare-bin
	@if [[ ! -x "${GET_VERSION_BIN}" ]] || [[ "$$("${GET_VERSION_BIN}" -version 2>/dev/null)" != "${GET_VERSION_VERSION}" ]]; then
		echo "Installing get-version ${GET_VERSION_VERSION}"
		curl -sSLo /tmp/get-version.zip https://github.com/scylladb-actions/get-version/releases/download/v${GET_VERSION_VERSION}/get-version_${GET_VERSION_VERSION}_linux_amd64v3.zip
		unzip -o /tmp/get-version.zip get-version -d "$(MAKEFILE_PATH)/bin" >/dev/null
	fi

.prepare-environment-update-aio-max-nr:
	@if (( $$(< /proc/sys/fs/aio-max-nr) < 2097152 )); then
		echo 2097152 | sudo tee /proc/sys/fs/aio-max-nr >/dev/null
	fi

clean-old-temporary-docker-images:
	@echo "Running Docker Hub image cleanup script..."
	python ci/clean-old-temporary-docker-images.py

CASSANDRA_VERSION_FILE=/tmp/cassandra-version-${CASSANDRA_VERSION}.resolved
resolve-cassandra-version: .prepare-get-version
	@find "${CASSANDRA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1 || true
	if [[ -f "${CASSANDRA_VERSION_FILE}" ]]; then
		echo "Resolved Cassandra ${CASSANDRA_VERSION} to $$(cat ${CASSANDRA_VERSION_FILE})"
		exit 0
	fi

	if [[ "${CASSANDRA_VERSION}" == "LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=`get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST" | tr -d '\"'`
	elif [[ "${CASSANDRA_VERSION}" == "5-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=`get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 5.LAST.LAST" | tr -d '\"'`
	elif [[ "${CASSANDRA_VERSION}" == "4-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=`get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 4.LAST.LAST" | tr -d '\"'`
	elif [[ "${CASSANDRA_VERSION}" == "3-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=`get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 3.LAST.LAST" | tr -d '\"'`
	elif echo "${CASSANDRA_VERSION}" | grep -P '^[0-9\.]+'; then
		CASSANDRA_VERSION_RESOLVED=${CASSANDRA_VERSION}
	else
		echo "Unknown Cassandra version name '${CASSANDRA_VERSION}'"
		exit 1
	fi

	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "There is no ${CASSANDRA_VERSION} Cassandra version"
		if [[ -n "$${GITHUB_ENV}" ]]; then
			echo "value=NOT-FOUND" >>$${GITHUB_OUTPUT}
			echo "CASSANDRA_VERSION_RESOLVED=NOT-FOUND" >>$${GITHUB_ENV}
			exit 0
		fi
		exit 2
	fi

	echo "Resolved Cassandra ${CASSANDRA_VERSION} to $${CASSANDRA_VERSION_RESOLVED}"
	if [[ -n "$${GITHUB_OUTPUT}" ]]; then
		echo "value=$${CASSANDRA_VERSION_RESOLVED}" >>$${GITHUB_OUTPUT}
	fi
	if [[ -n "$${GITHUB_ENV}" ]]; then
		echo "CASSANDRA_VERSION_RESOLVED=$${CASSANDRA_VERSION_RESOLVED}" >>$${GITHUB_ENV}
	fi
	echo "$${CASSANDRA_VERSION_RESOLVED}" >${CASSANDRA_VERSION_FILE}

SCYLLA_VERSION_FILE=/tmp/scylla-version-${SCYLLA_VERSION}.resolved
resolve-scylla-version: .prepare-get-version
	@find "${SCYLLA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1 || true
	if [[ -f "${SCYLLA_VERSION_FILE}" ]]; then
		echo "Resolved ScyllaDB ${SCYLLA_VERSION} to $$(cat ${SCYLLA_VERSION_FILE})"
		exit 0
	fi

	if [[ "${SCYLLA_VERSION}" == "LTS-LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=`get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.1.LAST" | tr -d '\"'`
	elif [[ "${SCYLLA_VERSION}" == "LTS-PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=`get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST-1.1.LAST" | tr -d '\"'`
		if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
			SCYLLA_VERSION_RESOLVED=`get-version --source dockerhub-imagetag --repo scylladb/scylla-enterprise -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST-1.1.LAST" | tr -d '\"'`
		fi
	elif [[ "${SCYLLA_VERSION}" == "LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=`get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST" | tr -d '\"'`
	elif [[ "${SCYLLA_VERSION}" == "PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=`get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST-1" | tr -d '\"'`
	elif echo "${SCYLLA_VERSION}" | grep -P '^[0-9\.]+'; then
		SCYLLA_VERSION_RESOLVED=${SCYLLA_VERSION}
	else
		echo "Unknown ScyllaDB version name '${SCYLLA_VERSION}'"
		exit 1
	fi

	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "There is no ${SCYLLA_VERSION} ScyllaDB version"
		if [[ -n "$${GITHUB_ENV}" ]]; then
			echo "value=NOT-FOUND" >>$${GITHUB_OUTPUT}
			echo "SCYLLA_VERSION_RESOLVED=NOT-FOUND" >>$${GITHUB_ENV}
			exit 0
		fi
		exit 2
	fi

	echo "Resolved ScyllaDB ${SCYLLA_VERSION} to $${SCYLLA_VERSION_RESOLVED}"
	if [[ -n "$${GITHUB_OUTPUT}" ]]; then
		echo "value=$${SCYLLA_VERSION_RESOLVED}" >>$${GITHUB_OUTPUT}
	fi
	if [[ -n "$${GITHUB_ENV}" ]]; then
		echo "SCYLLA_VERSION_RESOLVED=$${SCYLLA_VERSION_RESOLVED}" >>$${GITHUB_ENV}
	fi
	echo "$${SCYLLA_VERSION_RESOLVED}" >${SCYLLA_VERSION_FILE}

cassandra-start: .prepare-pki .prepare-cassandra-ccm .prepare-java resolve-cassandra-version
	@if [ -d ${CCM_CONFIG_DIR}/${CCM_CASSANDRA_CLUSTER_NAME} ] && ccm switch ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 && ccm status | grep UP 2>/dev/null 1>&2; then
		echo "Cassandra cluster is already started"
		exit 0
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(cat '${CASSANDRA_VERSION_FILE}')
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Cassandra version ${CASSANDRA_VERSION} was not resolved"
		exit 1
	fi
	source ~/.sdkman/bin/sdkman-init.sh;
	echo "Start Cassandra ${CASSANDRA_VERSION}($${CASSANDRA_VERSION_RESOLVED}) cluster"
	ccm stop ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	ccm remove ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	ccm create ${CCM_CASSANDRA_CLUSTER_NAME} -i ${CCM_CASSANDRA_IP_PREFIX} -v "$${CASSANDRA_VERSION_RESOLVED}" -n3 -d --vnodes --jvm_arg="-Xmx256m -XX:NewSize=100m"
	ccm updateconf ${CASSANDRA_CONFIG}
	for conf_dir in ${CCM_CONFIG_DIR}/${CCM_CASSANDRA_CLUSTER_NAME}/node*/conf; do \
		sed -i 's/^#MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE="256M"/' "$$conf_dir/cassandra-env.sh"; \
	done
	ccm start --wait-for-binary-proto --wait-other-notice --verbose
	ccm status
	ccm node1 nodetool status

scylla-start: .prepare-pki .prepare-scylla-ccm .prepare-environment-update-aio-max-nr resolve-scylla-version
	@if [ -d ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME} ] && ccm switch ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 && ccm status | grep UP 2>/dev/null 1>&2; then
		echo "Scylla cluster is already started";
		exit 0;
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(cat '${SCYLLA_VERSION_FILE}')
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "ScyllaDB version ${SCYLLA_VERSION} was not resolved"
		exit 1
	fi
	echo "Start scylla $(SCYLLA_VERSION)($${SCYLLA_VERSION_RESOLVED}) cluster"
	ccm stop ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	ccm remove ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	if [[ "$${SCYLLA_VERSION_RESOLVED}" != *:* ]]; then
		SCYLLA_VERSION_RESOLVED="release:$${SCYLLA_VERSION_RESOLVED}"
	fi
	ccm create ${CCM_SCYLLA_CLUSTER_NAME} -i ${CCM_SCYLLA_IP_PREFIX} --scylla -v $${SCYLLA_VERSION_RESOLVED} -n 3 -d --jvm_arg="--smp 2 --memory 1G --experimental-features udf --enable-user-defined-functions true"
	ccm updateconf ${SCYLLA_CONFIG}
	ccm start --wait-for-binary-proto --wait-other-notice --verbose
	ccm status
	ccm node1 nodetool status
	sudo chmod 0777 ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/*/cql.m || true

download-cassandra: .prepare-cassandra-ccm resolve-cassandra-version
	@if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(cat '${CASSANDRA_VERSION_FILE}')
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Cassandra version ${CASSANDRA_VERSION} was not resolved"
		exit 1
	fi
	rm -rf /tmp/download.ccm || true
	mkdir /tmp/download.ccm || true
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$${CASSANDRA_VERSION_RESOLVED}" --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

download-scylla: .prepare-scylla-ccm resolve-scylla-version
	@if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(cat '${SCYLLA_VERSION_FILE}')
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "ScyllaDB version ${SCYLLA_VERSION} was not resolved"
		exit 1
	fi
	rm -rf /tmp/download.ccm || true
	mkdir /tmp/download.ccm || true
	if [[ "$${SCYLLA_VERSION_RESOLVED}" != *:* ]]; then
		SCYLLA_VERSION_RESOLVED="release:$${SCYLLA_VERSION_RESOLVED}"
	fi
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$${SCYLLA_VERSION_RESOLVED}" --scylla --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

cassandra-stop: .prepare-cassandra-ccm
	@echo "Stop cassandra cluster"
	@ccm stop --not-gently ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	@ccm remove ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true

scylla-stop: .prepare-scylla-ccm
	@echo "Stop scylla cluster"
	@ccm stop --not-gently ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	@ccm remove ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true

# The package pattern below is "." (this package only), not "./...". All
# integration-tagged test files live in this package; the handful of
# unconditional (untagged) test files in other packages -- dialer,
# hostpolicy, several serialization/* packages, etc. -- would also get
# built and would fail to parse -distribution/-cluster/etc, which only this
# package's test files register. Cross-package coverage attribution still
# works without testing those packages directly: -coverpkg=./... controls
# what gets *instrumented*, separately from what gets *run*, so code in
# other packages exercised transitively through this package's tests is
# still measured under test-integration-*-coverage.
test-integration-cassandra: cassandra-start
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(cat '${CASSANDRA_VERSION_FILE}')
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Cassandra version ${CASSANDRA_VERSION} was not resolved"
		exit 1
	fi
	echo "go test -v ${TEST_OPTS} -tags \"${TEST_INTEGRATION_TAGS}\" ${COVER_BUILD_ARGS} -timeout=10m . -args -distribution cassandra -runauth -gocql.timeout=60s -runssl -proto=${TEST_CQL_PROTOCOL} -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=$${CASSANDRA_VERSION_RESOLVED} -cluster=$$(ccm liveset) ${COVER_RUNTIME_ARGS}"
	go test -v ${TEST_OPTS} -tags "${TEST_INTEGRATION_TAGS}" ${COVER_BUILD_ARGS} -timeout=10m . -args -distribution cassandra -runauth -gocql.timeout=60s -runssl -proto=${TEST_CQL_PROTOCOL} -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=$$(ccm node1 versionfrombuild) -cluster=$$(ccm liveset) ${COVER_RUNTIME_ARGS}

test-integration-scylla: scylla-start
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on scylla ${SCYLLA_VERSION}"
	if [ -S "${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node1/cql.m" ]; then
		CLUSTER_SOCKET="-cluster-socket ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node1/cql.m"
	else
		echo "Cluster socket is not found"
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(cat '${SCYLLA_VERSION_FILE}')
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "ScyllaDB version ${SCYLLA_VERSION} was not resolved"
		exit 1
	fi
	echo "go test -v ${TEST_OPTS} -tags \"${TEST_INTEGRATION_TAGS}\" ${COVER_BUILD_ARGS} -timeout=5m . -args -distribution scylla $${CLUSTER_SOCKET} -gocql.timeout=60s -proto=${TEST_CQL_PROTOCOL} -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=$${SCYLLA_VERSION_RESOLVED} -cluster=$$(ccm liveset) ${COVER_RUNTIME_ARGS}"
	go test -v ${TEST_OPTS} -tags "${TEST_INTEGRATION_TAGS}" ${COVER_BUILD_ARGS} -timeout=5m . -args -distribution scylla $${CLUSTER_SOCKET} -gocql.timeout=60s -proto=${TEST_CQL_PROTOCOL} -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=$${SCYLLA_VERSION_RESOLVED} -cluster=$$(ccm liveset) ${COVER_RUNTIME_ARGS}

# The lz4 compressor lives in a nested module (lz4/go.mod), so the root "./..."
# pattern does not reach it — it has to be invoked explicitly with `go test -C`,
# the same way check-go-mod-drift lists every module. Without that, the lz4
# tests and benchmarks are green or red independently of CI.
test-unit: .prepare-pki
	@echo "Run unit tests"
	go clean -testcache
	go clean -C lz4 -testcache
ifeq ($(shell if [[ -n "$${GITHUB_STEP_SUMMARY}" ]]; then echo "running-in-workflow"; else echo "running-in-shell"; fi), running-in-workflow)
	echo "### Unit Test Results" >>$${GITHUB_STEP_SUMMARY}
	echo '```' >>$${GITHUB_STEP_SUMMARY}
	echo go test -tags unit -timeout=5m -race ./... ${COVER_ARGS}
	TEST_STATUS=0
	go test -tags unit -timeout=5m -race ./... ${COVER_ARGS} | tee -a "$${GITHUB_STEP_SUMMARY}" || TEST_STATUS=$${PIPESTATUS[0]}
	echo go test -C lz4 -tags unit -timeout=5m -race ./... ${COVER_ARGS}
	LZ4_TEST_STATUS=0
	go test -C lz4 -tags unit -timeout=5m -race ./... ${COVER_ARGS} | tee -a "$${GITHUB_STEP_SUMMARY}" || LZ4_TEST_STATUS=$${PIPESTATUS[0]}
	echo '```' >>"$${GITHUB_STEP_SUMMARY}"
	if (( TEST_STATUS != 0 )); then exit "$${TEST_STATUS}"; fi
	exit "$${LZ4_TEST_STATUS}"
else
	go test -v -tags unit -timeout=5m -race ./... ${COVER_ARGS}
	go test -C lz4 -v -tags unit -timeout=5m -race ./... ${COVER_ARGS}
endif

.prepare-coverage-dir:
	@mkdir -p "${COVERAGE_DIR}"

# Coverage-instrumented variants of the targets above. Each recurses into the
# original target with COVER_ARGS/COVER_BUILD_ARGS/COVER_RUNTIME_ARGS set,
# rather than duplicating its recipe, so CCM setup, version resolution and
# GITHUB_STEP_SUMMARY handling stay in one place. Every *-coverage target
# writes into the same COVERAGE_DIR, so unit and integration runs (and
# multiple integration runs against different clusters/tags) all accumulate
# into one combined report -- see coverage-report.
#
# COVERAGE_DIR is quoted (with an escaped \" so the quote survives being
# re-expanded, unquoted, inside the target recipe's own `go test` line) so a
# path containing spaces isn't word-split by the shell into several
# arguments.
#
# -covermode=atomic is explicit (rather than relying on -race, which forces
# it) because `go tool covdata` refuses to merge data recorded under
# different counter modes ("counter mode clash"): every *-coverage target
# and ccm-test-coverage need to agree, not just test-unit-coverage, which is
# the only one that would otherwise get atomic mode for free from -race.
test-unit-coverage: .prepare-coverage-dir
	@$(MAKE) test-unit COVER_ARGS="-cover -covermode=atomic -coverpkg=./... -args -test.gocoverdir=\"${COVERAGE_DIR}\""

test-integration-scylla-coverage: .prepare-coverage-dir
	@$(MAKE) test-integration-scylla COVER_BUILD_ARGS="-cover -covermode=atomic -coverpkg=./..." COVER_RUNTIME_ARGS="-test.gocoverdir=\"${COVERAGE_DIR}\""

test-integration-cassandra-coverage: .prepare-coverage-dir
	@$(MAKE) test-integration-cassandra COVER_BUILD_ARGS="-cover -covermode=atomic -coverpkg=./..." COVER_RUNTIME_ARGS="-test.gocoverdir=\"${COVERAGE_DIR}\""

# internal/ccm's tests (the only tests the "ccm" build tag selects -- nothing
# else in the module has a file gated by it) don't take any of the custom
# flags test-integration-scylla passes (-distribution, -cluster, ...); its
# test binary doesn't define them and would fail to parse them. It also needs
# to be targeted directly rather than through ./...: `go test` stops
# resolving its own flags (including the package pattern) at the first flag
# it doesn't recognize, so a ./... alongside those custom flags silently
# collapses to just ".", which is why reusing test-integration-scylla for
# this never actually ran internal/ccm's tests.
ccm-test-coverage: .prepare-coverage-dir
	@go test -tags "ccm gocql_debug" -timeout=5m -v -cover -covermode=atomic -coverpkg=./... ./internal/ccm/... -args -test.gocoverdir="${COVERAGE_DIR}"

# lz4 is a separate Go module (see its go.mod), so a single `go tool covdata`
# invocation can't render both it and the root module together -- `go tool
# cover` resolves source files against the module rooted at the current
# directory, and a merged profile spanning two modules fails with "no
# required module provides package ...". Filter per module with `-pkg` and
# run `go tool cover` from within each module's own directory instead.
coverage-report:
	@echo "Generate coverage report"
	go tool covdata textfmt -i="${COVERAGE_DIR}" -pkg="github.com/gocql/gocql/..." -o="${MAKEFILE_PATH}/coverage-root.out"
	go tool covdata textfmt -i="${COVERAGE_DIR}" -pkg="github.com/scylladb/gocql/lz4/..." -o="${MAKEFILE_PATH}/coverage-lz4.out"
ifeq ($(shell if [[ -n "$${GITHUB_STEP_SUMMARY}" ]]; then echo "running-in-workflow"; else echo "running-in-shell"; fi), running-in-workflow)
	echo "### Coverage Report" >>$${GITHUB_STEP_SUMMARY}
	echo '```' >>$${GITHUB_STEP_SUMMARY}
	go tool covdata percent -i="${COVERAGE_DIR}" -pkg="github.com/gocql/gocql/..." | tee -a "$${GITHUB_STEP_SUMMARY}"
	go tool cover -func="${MAKEFILE_PATH}/coverage-root.out" | tail -1 | sed 's/^total:/root module total:/' | tee -a "$${GITHUB_STEP_SUMMARY}"
	go tool covdata percent -i="${COVERAGE_DIR}" -pkg="github.com/scylladb/gocql/lz4/..." | tee -a "$${GITHUB_STEP_SUMMARY}"
	(cd lz4 && go tool cover -func="${MAKEFILE_PATH}/coverage-lz4.out") | tail -1 | sed 's/^total:/lz4 module total:/' | tee -a "$${GITHUB_STEP_SUMMARY}"
	echo '```' >>"$${GITHUB_STEP_SUMMARY}"
else
	go tool covdata percent -i="${COVERAGE_DIR}" -pkg="github.com/gocql/gocql/..."
	go tool cover -func="${MAKEFILE_PATH}/coverage-root.out" | tail -1 | sed 's/^total:/root module total:/'
	go tool covdata percent -i="${COVERAGE_DIR}" -pkg="github.com/scylladb/gocql/lz4/..."
	(cd lz4 && go tool cover -func="${MAKEFILE_PATH}/coverage-lz4.out") | tail -1 | sed 's/^total:/lz4 module total:/'
endif
	go tool cover -html="${MAKEFILE_PATH}/coverage-root.out" -o="${MAKEFILE_PATH}/coverage-root.html"
	(cd lz4 && go tool cover -html="${MAKEFILE_PATH}/coverage-lz4.out" -o="${MAKEFILE_PATH}/coverage-lz4.html")

clean-coverage:
	rm -rf "${COVERAGE_DIR}" "${MAKEFILE_PATH}"/coverage-root.* "${MAKEFILE_PATH}"/coverage-lz4.*

# The benchmarks are behind the `bench` build tag (and, in the root module, some
# behind `unit`), so the tags have to be passed or `go test -bench` compiles and
# runs almost nothing. `-run '^$$'` keeps this a benchmark-only run; the tests
# themselves belong to test-unit.
BENCH_OPTS = -tags "bench unit" -run '^$$' -bench=. -benchmem

test-bench:
	@echo "Run benchmark tests"
ifeq ($(shell if [[ -n "$${GITHUB_STEP_SUMMARY}" ]]; then echo "running-in-workflow"; else echo "running-in-shell"; fi), running-in-workflow)
	echo "### Benchmark Results" >>$${GITHUB_STEP_SUMMARY}
	echo '```' >>"$${GITHUB_STEP_SUMMARY}"
	echo go test ${BENCH_OPTS} ./...
	go test ${BENCH_OPTS} ./... | tee -a >>"$${GITHUB_STEP_SUMMARY}"
	echo go test -C lz4 ${BENCH_OPTS} ./...
	go test -C lz4 ${BENCH_OPTS} ./... | tee -a >>"$${GITHUB_STEP_SUMMARY}"
	echo '```' >>"$${GITHUB_STEP_SUMMARY}"
else
	go test ${BENCH_OPTS} ./...
	go test -C lz4 ${BENCH_OPTS} ./...
endif

check-go-mod-drift:
	@echo "Check Go module drift"
	go mod tidy -diff
	go mod tidy -C lz4 -diff
	go mod tidy -C tests/bench -diff

check: .prepare-golangci check-go-mod-drift
	@echo "Build"
	go build -tags all .
	echo "Check linting"
	${BIN_DIR}/golangci-lint run

fix-go-mod-drift:
	@echo "Fix Go module drift"
	go mod tidy
	go mod tidy -C lz4
	go mod tidy -C tests/bench

fix: .prepare-golangci fix-go-mod-drift
	@echo "Fix linting"
	${BIN_DIR}/golangci-lint run --fix

.prepare-java:
ifeq ($(shell if [ -f ~/.sdkman/bin/sdkman-init.sh ]; then echo "installed"; else echo "not-installed"; fi), not-installed)
	@$(MAKE) install-java
endif

install-java:
	@echo "Installing SDKMAN..."
	curl -s "https://get.sdkman.io" | bash
	echo "sdkman_auto_answer=true" >> ~/.sdkman/etc/config
	source ~/.sdkman/bin/sdkman-init.sh;
	echo "Installing Java versions...";
	sdk install java 11.0.30-zulu;
	sdk install java 17.0.18-zulu;
	sdk default java 11.0.30-zulu;
	sdk use java 11.0.30-zulu;

.prepare-cassandra-ccm:
	@if command -v ccm >/dev/null 2>&1 && grep CASSANDRA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_CASSANDRA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then
		echo "Cassandra CCM ${CCM_CASSANDRA_VERSION} is already installed";
		exit 0
	fi
	$(MAKE) install-cassandra-ccm

install-cassandra-ccm:
	@echo "Install CCM ${CCM_CASSANDRA_VERSION}"
	pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.prepare-scylla-ccm:
	@if command -v ccm >/dev/null 2>&1 && grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then
		echo "Scylla CCM ${CCM_SCYLLA_VERSION} is already installed";
		exit 0
	fi
	$(MAKE) install-scylla-ccm

install-scylla-ccm:
	@echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"
	pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.prepare-pki:
	@[ -f "testdata/pki/cassandra.key" ] || (echo "Generating new PKI" && cd testdata/pki/ && bash ./generate_certs.sh)

generate-pki:
	@echo "Generating new PKI"
	rm -f testdata/pki/.keystore testdata/pki/.truststore testdata/pki/*.p12 testdata/pki/*.key testdata/pki/*.crt || true
	cd testdata/pki/ && bash ./generate_certs.sh

.prepare-golangci:
	@if ! "${BIN_DIR}/golangci-lint" --version | grep '${GOLANGCI_VERSION}' >/dev/null 2>&1 ; then
		mkdir -p "${BIN_DIR}"
		echo "Installing golangci-lint to '${BIN_DIR}'"
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b bin/ v$(GOLANGCI_VERSION)
	fi
