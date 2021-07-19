#!/bin/bash
set -x
set -e

export CLICKHOUSE_BACKUP_BIN="$(pwd)/clickhouse-backup/clickhouse-backup"
export LOG_LEVEL=${LOG_LEVEL:-info}
export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-20.3}

if [[ "${CLICKHOUSE_VERSION}" == 2* ]]; then
  export COMPOSE_FILE=docker-compose_advanced.yml
else
  export COMPOSE_FILE=docker-compose.yml
fi

docker-compose -f test/integration/${COMPOSE_FILE} down --remove-orphans
docker volume prune -f
make clean
make build
docker-compose -f test/integration/${COMPOSE_FILE} up -d --force-recreate
sleep 5
# To run integration tests including GCS tests set GCS_TESTS environment variable:
# GCS_TESTS=true go test -tags=integration -v test/integration/integration_test.go
go test  -timeout 30m -failfast -tags=integration -run "${RUN_TESTS:-.+}" -v test/integration/integration_test.go
