# Migration Notes

**Model self-assessed confidence:** 50% (not a measured result)  
**Build at time of writing:** failed  
**Target unit tests:** failed  
**Behavior compared with the original:** mismatches

The final recommendation is in the pull request description.

---

## What was migrated

- `app/config.py` → `internal/config/config.go` (86% confidence)
- `app/db.py` → `internal/store/db.go` (76% confidence) ⚠️ needs review
- `app/catalog.py` → `internal/catalog/catalog.go` (87% confidence)
- `app/audit.py` → `internal/audit/audit.go` (85% confidence)
- `app/events.py` → `internal/events/publisher.go` (85% confidence) ⚠️ needs review
- `main.py` → `cmd/api/main.go` (62% confidence) ⚠️ needs review
- `app/models.py` → `internal/store/models.go` (81% confidence) ⚠️ needs review
- `app/worker.py` → `internal/worker/worker.go` (80% confidence) ⚠️ needs review
- `app/routes.py` → `internal/httpapi/handlers.go` (74% confidence) ⚠️ needs review

## Not migrated, or migrated with open issues

- `app/__init__.py`: ModuleStatus.completed_with_open_issues
## Files requiring manual review

These files were migrated but scored below the confidence threshold.
Review them carefully before merging.

### `app/db.py`
Confidence: 76%
Issues:
  - [warning] No call site of EnsureSchema or store.Open found in the target codebase; can't confirm the schema is created at boot.
  - [warning] There are no defaults in the database. Every Go insert must set these values explicitly.
  - [info] Approximated with a 5-minute idle-connection recycle plus Go's retry on bad connections. Connections are not checked before each use.

### `app/events.py`
Confidence: 85%

### `main.py`
Confidence: 62%
Issues:
  - [warning] Nothing calls newHTTPServer/listenAddr in this file. cmd/api/main.go's run() still builds its own http.Server with Addr cfg.Addr() (":<port>"), so the 0.0.0.0 host and the extra timeouts never take effect. Binding ":port" still listens on all interfaces, so clients in a container can reach the server either way.

### `app/models.py`
Confidence: 81%
Issues:
  - [warning] The Go code holds money values in float64 instead of a decimal type. NUMERIC values are converted to float when read and formatted with 2 decimal places when written. This can cause rounding drift in credit arithmetic (used + total <= limit) at edge values.

### `app/worker.py`
Confidence: 80%
Issues:
  - [warning] No worker entry point found (no cmd/worker main calling worker.Run, and no equivalent of init_db at startup). Run/consume logic itself matches: durable declare, manual ack after handle, 2s reconnect.
  - [info] Malformed JSON or missing order_id returns an error and the message is nacked (dropped). It does not crash the consumer, unlike the source. This change is deliberate and documented.

### `app/routes.py`
Confidence: 74%
Issues:
  - [warning] The handlers depend on Configure(Deps{...}) being called at startup with a CreditStore that implements ChargeCredit (e.g. NewSQLCreditStore). I couldn't confirm this: searching the target found no matches for 'Configure(' or 'RegisterRoutes', so this file appears not to be indexed. Without the wiring, every route except /health returns 500.
  - [info] Python round() rounds halves to even, but Go's math.Round rounds them away from zero. Totals can differ by one cent when they land exactly on a half cent.
