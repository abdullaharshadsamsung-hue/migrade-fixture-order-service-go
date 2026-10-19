# Order Service (Go)

Order Service is a backend HTTP service for handling orders. It is being migrated from a Python/Flask codebase to Go using the standard library.

> ⚠️ **Status: migration incomplete. Not ready for use.**
>
> | Check | Result |
> |---|---|
> | Build | ❌ Failed |
> | Target unit tests | ❌ Failed |
> | Behavior compared with the original Flask service | ❌ Mismatches found |
> | Modules migrated | 9 of 10 |
> | Migration confidence | ~50% (the migration tool's own estimate, not a measured result) |
>
> The steps below describe how the project is *intended* to be built and run. They will not produce a working service until the build errors are fixed. See [Known limitations](#known-limitations) and [Manual review required](#manual-review-required).

---

## Tech stack

- **Language:** Go. The required version is declared in `go.mod`.
- **HTTP layer:** Go standard library (no web framework).
- **Dependency management:** Go modules (`go.mod`, `go.sum`).
- **Containerization:** `Dockerfile` and `docker-compose.yml` are included. They have not been verified as part of this migration.

---

## Prerequisites

- The Go toolchain, at the version declared in `go.mod`.
- A database reachable through `DATABASE_URL`. The database engine is not documented by the migration. Check `internal/store/db.go` to determine what it expects.
- A POSIX shell. The install command uses `if [ ... ]` syntax.

---

## Getting started

Run these steps in order from a fresh clone.

### 1. Install dependencies and build

```sh
go mod download && if [ -d ./cmd/server ]; then mkdir -p /app/bin && go build -o /app/bin/server ./cmd/server; else echo 'WARNING: /app/cmd/server not present (only go.mod is in the mounted project); skipping build'; fi
```

Notes:

- The command assumes the project is located at `/app`, as it is in a container.
- If you run it outside a container, you need write access to `/app/bin`.
- The command builds only `./cmd/server`. It does not build `cmd/api` or `cmd/worker`.
- **This build currently fails.** The build errors must be resolved before you continue.

### 2. Configure environment variables

Export the variables listed in [Environment variables](#environment-variables):

```sh
export DATABASE_URL=...
export PORT=...
export JWT_SECRET=...
```

### 3. Database setup

The migration did not detect a database setup or schema migration command.

- Inspect `internal/store/db.go` and `internal/store/models.go` to find out what schema the service expects.
- Create that schema manually in the database referenced by `DATABASE_URL`.

### 4. Additional setup commands

None. The migration did not need any extra dependency-resolution or fix commands.

### 5. Run

```sh
/app/bin/server
```

This step works only after the build in step 1 succeeds.

---

## Running tests

```sh
go test ./...
```

**This currently fails.** Test files in the project:

- `cmd/api/main_test.go`
- `internal/config/config_test.go`
- `internal/store/db_test.go`

---

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | Connection string for the database used by `internal/store`. |
| `PORT` | Yes | Port the HTTP server listens on. |
| `JWT_SECRET` | Yes | Secret used for JWT signing/verification. |

The descriptions above are inferred from the variable names. Confirm defaults and validation rules in `internal/config/config.go`.

---

## Architecture overview

```
cmd/
  api/
    main.go          # Entry point (api variant)
    main_test.go
    router.go        # Route registration (api variant)
  server/
    main.go          # Entry point built by the install command
    router.go        # Route registration (server variant)
  worker/
    main.go          # Background worker entry point
internal/
  audit/audit.go         # Audit logging
  catalog/catalog.go     # Catalog/product logic
  config/config.go       # Environment-based configuration
  config/config_test.go
  events/publisher.go    # Event publishing
  httpapi/handlers.go    # HTTP handlers
  store/db.go            # Database access
  store/db_test.go
  store/models.go        # Data models
  worker/worker.go       # Worker logic
Dockerfile
docker-compose.yml
go.mod
go.sum
```

The component descriptions are based on file and package names. Verify them against the code.

### Entry points

There are **two** HTTP entry points:

- `cmd/api` (`main.go` and `router.go`)
- `cmd/server` (`main.go` and `router.go`)

Only `cmd/server` is built and run by the documented commands. Decide which one is canonical and remove or merge the other. See [Manual review required](#manual-review-required).

The worker (`cmd/worker`) has no documented build or run command.

---

## Migration notes

These are the main changes from the original Flask codebase:

- **HTTP stack:** Flask routing and request handling were replaced by the Go standard library. Routes live in the `router.go` files, and handlers live in `internal/httpapi/handlers.go`.
- **Application entry point:** The original entry point is replaced by the Go `main` packages under `cmd/`.
- **Package layout:** Python modules under `app/` are now Go packages under `internal/`.
- **Configuration:** Configuration is read from environment variables in `internal/config/config.go`.

---

## Known limitations

- **The build fails.** The service cannot currently be compiled or run.
- **The unit tests fail.** `go test ./...` does not pass.
- **Behavior does not match the original.** Comparison with the Flask service found mismatches. Do not treat this service as a drop-in replacement.
- **One module is unaccounted for.** Only 9 of 10 modules were migrated, and no component was reported as unmigratable. The missing module has not been identified. Diff the original source tree against this project to find it.
- **There is no database setup step.** Schema creation and migrations are not automated.
- **Entry points are duplicated.** Both `cmd/api` and `cmd/server` exist.
- **The worker has no documented build or run step.** `cmd/worker` is not built by the install command.
- **The container setup is unverified.** `Dockerfile` and `docker-compose.yml` have not been tested.
- **The install command assumes a container layout.** It hardcodes `/app/bin`.

---

## Manual review required

The migration flagged the following areas as low confidence. Review each one:

| Go files to review | What to check |
|---|---|
| `cmd/server/main.go`, `cmd/api/main.go` | Startup order, config loading, which entry point is canonical |
| `internal/httpapi/handlers.go`, `cmd/server/router.go`, `cmd/api/router.go` | Route paths, HTTP methods, status codes, JSON response shapes, auth/JWT handling |
| `internal/store/db.go` | Connection handling, transactions, query correctness |
| `internal/store/models.go` | Field names, types, nullability, JSON serialization |
| `internal/events/publisher.go` | Event names, payloads, delivery semantics |
| `internal/worker/worker.go`, `cmd/worker/main.go` | Job processing, retries, shutdown behavior |

Also review:

- `internal/config/config.go`: handling of `DATABASE_URL`, `PORT`, and `JWT_SECRET`, including defaults and missing-value behavior.
- `internal/audit/audit.go` and `internal/catalog/catalog.go`: these were not flagged as low confidence, but they are covered by the overall behavior mismatches.
- `Dockerfile` and `docker-compose.yml`: confirm they build and run the intended binary or binaries.

### Suggested order of work

1. Fix the compile errors so the project builds.
2. Get `go test ./...` passing.
3. Resolve the duplicated `cmd/api` and `cmd/server` entry points.
4. Identify the unmigrated 10th module.
5. Re-run the behavior comparison against the Flask service.