#!/usr/bin/env bash
# Entry point executed INSIDE the .NET SDK runner container (see docker-compose.tests.yml).
# Runs each real-provider persistence test suite and reports a combined result.
# Not meant to be run directly on the host - use ./test-with-docker.sh instead.
set -uo pipefail

# One or more target frameworks to run (space-separated), e.g. "net6.0 net10.0".
read -r -a FRAMEWORKS <<< "${NSTORE_TEST_TFM:-net6.0 net10.0}"

# Structured results (.trx) and the console log are written here. This lives on the
# bind-mounted workspace, so the files survive container teardown and are readable on
# the host (matches the repo's ignored [Tt]est[Rr]esult*/ convention).
RESULTS_DIR="${NSTORE_TEST_RESULTS_DIR:-/workspace/TestResults}"

# Extra args forwarded to `dotnet test` (e.g. --filter "FullyQualifiedName~Polling"),
# passed in via the NSTORE_TEST_ARGS environment variable. Word-split on spaces.
read -r -a EXTRA_ARGS <<< "${NSTORE_TEST_ARGS:-}"

# Provider test projects to run (in-memory is covered by NStore.Core.Tests and is
# intentionally excluded here - these are the projects that need real providers).
PROJECTS=(
  "src/NStore.Persistence.Sqlite.Tests/NStore.Persistence.Sqlite.Tests.csproj"
  "src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj"
  "src/NStore.Persistence.MsSql.Tests/NStore.Persistence.MsSql.Tests.csproj"
)

run_all() {
  echo "=================================================================="
  echo " NStore provider tests"
  echo "   frameworks: ${FRAMEWORKS[*]}"
  echo "   mongodb   : ${NSTORE_MONGODB:-<unset>}"
  echo "   mssql     : ${NSTORE_MSSQL:+<set>}"
  echo "   results   : ${RESULTS_DIR}"
  echo "=================================================================="

  local failed=()
  for project in "${PROJECTS[@]}"; do
    local name
    name="$(basename "$(dirname "$project")")"
    for tfm in "${FRAMEWORKS[@]}"; do
      echo ""
      echo ">>> Testing ${name} (${tfm})"
      if dotnet test "$project" \
          --framework "$tfm" \
          --nologo \
          --logger "console;verbosity=normal" \
          --logger "trx;LogFileName=${name}.${tfm}.trx" \
          --results-directory "$RESULTS_DIR" \
          "${EXTRA_ARGS[@]}"; then
        echo "<<< ${name} (${tfm}): PASSED"
      else
        echo "<<< ${name} (${tfm}): FAILED"
        failed+=("${name} (${tfm})")
      fi
    done
  done

  echo ""
  echo "=================================================================="
  if [ ${#failed[@]} -eq 0 ]; then
    echo " All provider test suites passed."
    echo " Results (console log + .trx per suite): ${RESULTS_DIR}"
    echo "=================================================================="
    return 0
  fi

  local IFS=,
  echo " FAILED suites: ${failed[*]}"
  echo " See ${RESULTS_DIR}/run.log and the per-suite .trx files for details."
  echo "=================================================================="
  return 1
}

mkdir -p "$RESULTS_DIR"
rm -f "$RESULTS_DIR"/*.trx "$RESULTS_DIR"/run.log 2>/dev/null || true

# Tee the whole run to run.log while keeping live console output. Piping (rather than
# exec redirection) guarantees tee flushes before the script exits.
run_all | tee "$RESULTS_DIR/run.log"
exit "${PIPESTATUS[0]}"
