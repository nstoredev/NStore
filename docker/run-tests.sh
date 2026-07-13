#!/usr/bin/env bash
# Entry point executed INSIDE the .NET SDK runner container (see docker-compose.tests.yml).
# Runs each real-provider persistence test suite and reports a combined result.
# Not meant to be run directly on the host - use ./test-with-docker.sh instead.
set -uo pipefail

TFM="${NSTORE_TEST_TFM:-net10.0}"

# Provider test projects to run (in-memory is covered by NStore.Core.Tests and is
# intentionally excluded here - these are the projects that need real providers).
PROJECTS=(
  "src/NStore.Persistence.Sqlite.Tests/NStore.Persistence.Sqlite.Tests.csproj"
  "src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj"
  "src/NStore.Persistence.MsSql.Tests/NStore.Persistence.MsSql.Tests.csproj"
)

echo "=================================================================="
echo " NStore provider tests"
echo "   framework : ${TFM}"
echo "   mongodb   : ${NSTORE_MONGODB:-<unset>}"
echo "   mssql     : ${NSTORE_MSSQL:+<set>}"
echo "=================================================================="

# Extra args forwarded to `dotnet test` (e.g. --filter "FullyQualifiedName~Polling"),
# passed in via the NSTORE_TEST_ARGS environment variable. Word-split on spaces.
read -r -a EXTRA_ARGS <<< "${NSTORE_TEST_ARGS:-}"

failed=()
for project in "${PROJECTS[@]}"; do
  name="$(basename "$(dirname "$project")")"
  echo ""
  echo ">>> Testing ${name} (${TFM})"
  if dotnet test "$project" \
      --framework "$TFM" \
      --nologo \
      --logger "console;verbosity=normal" \
      "${EXTRA_ARGS[@]}"; then
    echo "<<< ${name}: PASSED"
  else
    echo "<<< ${name}: FAILED"
    failed+=("$name")
  fi
done

echo ""
echo "=================================================================="
if [ ${#failed[@]} -eq 0 ]; then
  echo " All provider test suites passed."
  echo "=================================================================="
  exit 0
fi

echo " FAILED suites: ${failed[*]}"
echo "=================================================================="
exit 1
