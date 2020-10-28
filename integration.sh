#!/bin/bash
#
# Copyright (C) 2017 ScyllaDB
#

SCYLLA_IMAGE=${SCYLLA_IMAGE}
AUTH=${AUTH}

set -eu -o pipefail

# Static IPs from docker-compose.yml
scylla_liveset="192.168.100.11,192.168.100.12"

function scylla_up() {
  local exec="docker-compose exec -T"

  echo "==> Running Scylla ${SCYLLA_IMAGE}"
  docker pull ${SCYLLA_IMAGE}
  docker-compose up -d

  echo "==> Waiting for CQL port"
  for s in $(docker-compose ps --services); do
    until v=$(${exec} ${s} cqlsh -e "DESCRIBE SCHEMA"); do
      echo ${v}
      docker-compose logs --tail 10 ${s}
      sleep 5
    done
  done
  echo "==> Waiting for CQL port done"
}

function scylla_down() {
  echo "==> Stopping Scylla"
  docker-compose down
}

function scylla_restart() {
  scylla_down
  scylla_up
}

function run_scylla_tests() {
  local clusterSize=2
  local cversion="3.11.4"
  local proto=4
  local args="-gocql.timeout=60s -proto=${proto} -rf=3 -clusterSize=${clusterSize} -autowait=2000ms -compressor=snappy -gocql.cversion=${cversion} -cluster=${scylla_liveset}) ./..."
  echo "Args: $args"

  scylla_restart

  local go_test="go test -v -timeout=5m -race ${args}"
  if [[ "${AUTH}" == true ]]; then
    ${go_test} -tags "integration gocql_debug" -run=TestAuthentication -runauth
  else
    ${go_test} -tags "cassandra scylla gocql_debug"
    scylla_restart
    ${go_test} -tags "integration scylla gocql_debug"
    scylla_restart
    ${go_test} -tags "ccm gocql_debug"
  fi
}

run_scylla_tests
