# Provider tests in Docker

Run the persistence test suites against **real** providers (MongoDB, SQL Server,
SQLite) in containers, so every developer tests the same versions with only Docker
installed — no local .NET SDK, MongoDB, or SQL Server required.

## Run

```bash
./test-with-docker.sh
```

This builds/starts the containers, waits for MongoDB and SQL Server to be healthy,
runs the three provider suites, prints a combined summary, and exits non-zero if any
suite fails. Containers are stopped automatically on exit.

Forward extra arguments straight to `dotnet test`:

```bash
./test-with-docker.sh --filter "FullyQualifiedName~Polling"
```

## Where to see failures

Results are written to `./TestResults/` (on the host, so they survive container
teardown; the directory is git-ignored):

- `run.log` — the full runner console output (clean; no DB server noise).
- `<Suite>.<tfm>.trx` — one structured result file per suite **and framework**, e.g.
  `NStore.Persistence.Mongo.Tests.net10.0.trx`, openable in Visual Studio / Rider or
  any `.trx` viewer.

Quick triage:

```bash
grep -E "Failed|error|FAILED suites" TestResults/run.log
```

The script also prints the results location and failing-suite hints when it finishes.

## What runs

| Suite                            | Provider   | Backing container                         |
| -------------------------------- | ---------- | ----------------------------------------- |
| `NStore.Persistence.Sqlite.Tests`| SQLite     | none (runs in the SDK runner)             |
| `NStore.Persistence.Mongo.Tests` | MongoDB    | `mongo:7`                                 |
| `NStore.Persistence.MsSql.Tests` | SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` |

The in-memory provider is intentionally excluded — it needs no external service and
is exercised by `NStore.Core.Tests`.

## Configuration

Set via environment variables (or a `.env` file next to the compose file):

| Variable            | Default                 | Purpose                                   |
| ------------------- | ----------------------- | ----------------------------------------- |
| `MSSQL_SA_PASSWORD` | `NStore_Test_Passw0rd!` | SQL Server `sa` password                  |
| `NSTORE_TEST_TFM`   | `net6.0 net10.0`        | Space-separated frameworks to run         |

Both target frameworks run by default. The runner image (`docker/Dockerfile.tests`)
is the .NET 10 SDK plus the .NET 6 shared runtime, so the `net6.0` test hosts can
execute. To run a single framework:

```bash
NSTORE_TEST_TFM=net10.0 ./test-with-docker.sh
```

## Notes / troubleshooting

- **Requirements:** Docker with the Compose plugin. SQL Server needs ~2 GB of RAM
  available to Docker.
- **First run is slow** — it pulls the MongoDB and SQL Server images, builds the
  runner image (SDK 10 + .NET 6 runtime), and restores NuGet packages. The NuGet cache
  and SQLite build output are kept in named volumes so later runs are faster.
- **SQLite on a bind mount:** the SQLite `.db` file is written to a container-local
  Docker volume rather than the bind-mounted source tree, because SQLite's POSIX file
  locking is unreliable over the host bind-mount (macOS/Windows) and would otherwise
  produce spurious `database is locked` errors.
- **Reset everything** (including cached volumes):
  `docker compose -f docker-compose.tests.yml down -v`
