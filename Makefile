SHELL := bash
MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
KEY_PATH = ${MAKEFILE_PATH}/testdata/pki

CASSANDRA_VERSION ?= 4.1.6
SCYLLA_VERSION ?= release:6.1.1

TEST_CQL_PROTOCOL ?= 4
TEST_COMPRESSOR ?= snappy
TEST_OPTS ?=
TEST_INTEGRATION_TAGS ?= integration gocql_debug
JVM_EXTRA_OPTS ?= -Dcassandra.test.fail_writes_ks=test -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler

CCM_CASSANDRA_CLUSTER_NAME = gocql_cassandra_integration_test
CCM_CASSANDRA_IP_PREFIX = 127.0.1.
CCM_CASSANDRA_REPO ?= github.com/apache/cassandra-ccm
CCM_CASSANDRA_VERSION ?= d3225ac6565242b231129e0c4f8f0b7a041219cf

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

ifeq ($(shell echo "${CASSANDRA_VERSION}" | grep -oP "3\.[0-9]+\.[0-9]+" ),${CASSANDRA_VERSION})
	CASSANDRA_CONFIG += "rpc_server_type: sync" \
"rpc_min_threads: 2" \
"rpc_max_threads: 2" \
"enable_user_defined_functions: true" \
"enable_materialized_views: true" \

else ifeq ($(shell echo "${CASSANDRA_VERSION}" | grep -oP "4\.0\.[0-9]+" ),${CASSANDRA_VERSION})
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

cassandra-start: .prepare-pki .prepare-cassandra-ccm .prepare-java
	@if [ -d ${CCM_CONFIG_DIR}/${CCM_CASSANDRA_CLUSTER_NAME} ] && ccm switch ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 && ccm status | grep UP 2>/dev/null 1>&2; then \
		echo "Cassandra cluster is already started"; \
  	else \
		echo "Start cassandra ${CASSANDRA_VERSION} cluster"; \
		ccm stop ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true; \
		ccm remove ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true; \
		ccm create ${CCM_CASSANDRA_CLUSTER_NAME} -i ${CCM_CASSANDRA_IP_PREFIX} -v ${CASSANDRA_VERSION} -n 3 -d --vnodes --jvm_arg="-Xmx256m -XX:NewSize=100m" && \
		ccm updateconf ${CASSANDRA_CONFIG} && \
		ccm start --wait-for-binary-proto --wait-other-notice --verbose && \
		ccm status && \
		ccm node1 nodetool status; \
  	fi

scylla-start: .prepare-pki .prepare-scylla-ccm .prepare-java
	@if [ -d ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME} ] && ccm switch ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 && ccm status | grep UP 2>/dev/null 1>&2; then \
		echo "Scylla cluster is already started"; \
  	else \
		echo "Start scylla ${SCYLLA_VERSION} cluster"; \
		ccm stop ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true; \
		ccm remove ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true; \
		ccm create ${CCM_SCYLLA_CLUSTER_NAME} -i ${CCM_SCYLLA_IP_PREFIX} --scylla -v ${SCYLLA_VERSION} -n 3 -d --jvm_arg="--smp 2 --memory 1G --experimental-features udf --enable-user-defined-functions true" && \
		ccm updateconf ${SCYLLA_CONFIG} && \
		ccm start --wait-for-binary-proto --wait-other-notice --verbose && \
		ccm status && \
		ccm node1 nodetool status && \
		sudo chmod 0777 ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node1/cql.m && \
		sudo chmod 0777 ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node2/cql.m && \
		sudo chmod 0777 ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node3/cql.m; \
	fi

cassandra-stop: .prepare-cassandra-ccm
	@echo "Stop cassandra cluster"
	@ccm stop --not-gently ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	@ccm remove ${CCM_CASSANDRA_CLUSTER_NAME} 2>/dev/null 1>&2 || true

scylla-stop: .prepare-scylla-ccm
	@echo "Stop scylla cluster"
	@ccm stop --not-gently ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	@ccm remove ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true

test-integration-cassandra: cassandra-start
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on cassandra ${CASSANDRA_VERSION}"
	go test -v ${TEST_OPTS} -tags "${TEST_INTEGRATION_TAGS}" -timeout=5m -runauth -gocql.timeout=60s -runssl -proto=${TEST_CQL_PROTOCOL} -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=$$(ccm node1 versionfrombuild) -cluster=$$(ccm liveset) ./...

test-integration-scylla: scylla-start
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on scylla ${SCYLLA_IMAGE}"
	go test -v ${TEST_OPTS} -tags "${TEST_INTEGRATION_TAGS}" -cluster-socket ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node1/cql.m -timeout=5m -gocql.timeout=60s -proto=${TEST_CQL_PROTOCOL} -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=$$(ccm node1 versionfrombuild) -cluster=$$(ccm liveset) ./...

test-unit: .prepare-pki
	@echo "Run unit tests"
	@go clean -testcache
ifeq ($(shell if [[ -n "$${GITHUB_STEP_SUMMARY}" ]]; then echo "running-in-workflow"; else echo "running-in-shell"; fi), running-in-workflow)
	@echo "### Unit Test Results" >>$${GITHUB_STEP_SUMMARY}
	@echo '```' >>$${GITHUB_STEP_SUMMARY}
	@echo go test -tags unit -timeout=5m -race ./...
	@go test -tags unit -timeout=5m -race ./... | tee -a $${GITHUB_STEP_SUMMARY}
	@echo '```' >>$${GITHUB_STEP_SUMMARY}
else
	go test -v -tags unit -timeout=5m -race ./...
endif

test-bench:
	@echo "Run benchmark tests"
ifeq ($(shell if [[ -n "$${GITHUB_STEP_SUMMARY}" ]]; then echo "running-in-workflow"; else echo "running-in-shell"; fi), running-in-workflow)
	@echo "### Benchmark Results" >>$${GITHUB_STEP_SUMMARY}
	@echo '```' >>$${GITHUB_STEP_SUMMARY}
	@echo go test -bench=. -benchmem ./...
	@go test -bench=. -benchmem ./... | tee -a >>$${GITHUB_STEP_SUMMARY}
	@echo '```' >>$${GITHUB_STEP_SUMMARY}
else
	go test -bench=. -benchmem ./...
endif

check:
	@echo "Run go vet linter"
	go vet --tags "unit all ccm cassandra integration" ./...

.prepare-java:
ifeq ($(shell if [ -f ~/.sdkman/bin/sdkman-init.sh ]; then echo "installed"; else echo "not-installed"; fi), not-installed)
	@$(MAKE) install-java
endif

install-java:
	@echo "Installing SDKMAN..."
	@curl -s "https://get.sdkman.io" | bash
	@echo "sdkman_auto_answer=true" >> ~/.sdkman/etc/config
	@( \
		source ~/.sdkman/bin/sdkman-init.sh; \
		export PATH=${PATH}:~/.sdkman/bin; \
		echo "Installing Java versions..."; \
		sdk install java 11.0.24-zulu; \
		sdk install java 17.0.12-zulu; \
		sdk default java 11.0.24-zulu; \
		sdk use java 11.0.24-zulu \
	)

.prepare-cassandra-ccm:
	@ccm --help 2>/dev/null 1>&2; if [[ $$? -lt 127 ]] && grep CASSANDRA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_CASSANDRA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then \
		echo "Cassandra CCM ${CCM_CASSANDRA_VERSION} is already installed"; \
  	else \
		echo "Installing Cassandra CCM ${CCM_CASSANDRA_VERSION}"; \
		pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"; \
		mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true; \
		echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type; \
		echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version; \
  	fi

install-cassandra-ccm: cassandra-start
	@echo "Install CCM ${CCM_CASSANDRA_VERSION}"
	@pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"
	@mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	@echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type
	@echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.prepare-scylla-ccm:
	@ccm --help 2>/dev/null 1>&2; if [[ $$? -lt 127 ]] && grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then \
		echo "Scylla CCM ${CCM_SCYLLA_VERSION} is already installed"; \
  	else \
		echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"; \
		pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"; \
		mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true; \
		echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type; \
		echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version; \
  	fi

install-scylla-ccm:
	@echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"
	@pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"
	@mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	@echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	@echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

.prepare-pki:
	@[ -f "testdata/pki/cassandra.key" ] || (echo "Generating new PKI" && cd testdata/pki/ && bash ./generate_certs.sh)

generate-pki:
	@echo "Generating new PKI"
	@rm -f testdata/pki/.keystore testdata/pki/.truststore testdata/pki/*.p12 testdata/pki/*.key testdata/pki/*.crt || true
	@cd testdata/pki/ && bash ./generate_certs.sh
