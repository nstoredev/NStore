#!/usr/bin/env bash
# Run the NStore provider test suites (MongoDB, SQL Server, SQLite) against real
# providers in Docker. The only prerequisite is Docker with the Compose plugin.
#
#   ./test-with-docker.sh
#   ./test-with-docker.sh --filter "FullyQualifiedName~Polling"   # forwarded to dotnet test
#
# Environment overrides:
#   MSSQL_SA_PASSWORD   SQL Server 'sa' password (default: NStore_Test_Passw0rd!)
#   NSTORE_TEST_TFM     target framework (default: net10.0)
set -euo pipefail

cd "$(dirname "$0")"
COMPOSE_FILE="docker-compose.tests.yml"

if ! docker compose version >/dev/null 2>&1; then
  echo "error: 'docker compose' is required (Docker Desktop or the compose plugin)." >&2
  exit 1
fi

# Extra args become the runner's `dotnet test` arguments.
if [ "$#" -gt 0 ]; then
  export NSTORE_TEST_ARGS="$*"
fi

cleanup() {
  # Stop and remove the containers/network. Named volumes (NuGet cache, SQLite build
  # output) are kept on purpose to speed up subsequent runs; DB data is ephemeral
  # (Mongo uses tmpfs, SQL Server lives in the container layer) so nothing leaks.
  docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p TestResults

# --exit-code-from tests makes the run exit with the test runner's status and tears
# down the DB containers as soon as the runner finishes. Capture the status so we can
# always point at the results, whether the run passed or failed.
rc=0
docker compose -f "$COMPOSE_FILE" up \
  --build \
  --abort-on-container-exit \
  --exit-code-from tests || rc=$?

echo ""
if [ "$rc" -eq 0 ]; then
  echo "PASSED. Results in ./TestResults (run.log + one .trx per suite)."
else
  echo "FAILED (exit ${rc}). Inspect ./TestResults/run.log or the per-suite .trx files:"
  echo "  - grep -E 'Failed|error|FAILED suites' TestResults/run.log"
  ls -1 TestResults/*.trx 2>/dev/null | sed 's/^/  - /' || true
fi
exit "$rc"
