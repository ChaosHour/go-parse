# Production Readiness Assessment — go-parse

**Date:** 2026-07-04
**Current state:** Builds cleanly, `go vet` passes, all tests pass (`cmd`, `cmd/scan`, `pkg/schema`, `pkg/stats`). The tool works, but there are correctness bugs, tooling gaps, and repo hygiene items to address before calling it production ready.

Items are ordered by priority. Each is small enough to knock out one at a time.

---

## P0 — Correctness bugs

### 1. ✅ DONE (4163b49) — Lost output in `go-parse-scan` detect-large mode (race condition)
`cmd/scan/main.go` (~line 1062): the printer goroutine ranges over the `out` channel, but `main()` closes the channel and returns immediately after `wg.Wait()`. The printer goroutine may still be draining/encoding events when the process exits — **large-event results can be silently dropped**, especially the last ones. Fix: add a `done` channel (or `sync.WaitGroup`) for the printer and wait for it after `close(out)`.

### 2. ✅ DONE (bfc9a3b) — Non-zero work, zero exit codes
Both tools print parse errors to stderr but always exit 0 (e.g. `cmd/main.go` end of `main()`, per-file `error parsing %s` in scan). Scripts and cron jobs can't detect failure. Fix: track failures and `os.Exit(1)` when any file fails to parse.

### 3. ✅ DONE (b8a6798) — `ExtractedCols` only captured for the first record per category/minute
`cmd/scan/main.go` `processFileAggregate`: when a record already exists only `Count++` runs; extracted column values from subsequent rows are ignored, and which row "wins" is nondeterministic under `-parallel`. At minimum document that extracted values come from the first-seen row; ideally make it deterministic.

## P1 — Code quality / lint

### 4. `gofmt` failures
Five files are not gofmt-clean: `cmd/main.go`, `cmd/scan/main.go`, `cmd/scan/main_test.go`, `pkg/stats/stats.go`, `pkg/stats/stats_test.go`. Fix: run `gofmt -w .` and add a CI check.

### 5. staticcheck finding
`pkg/schema/schema.go:91` — `currentDB` assigned but never used in the `USE` branch (SA4006). Review whether the `USE` statement is supposed to set the default database for subsequent unqualified `CREATE TABLE`s (it currently does not — `defaultDBName` only comes from the header comment). This is likely a latent bug, not just lint noise.

### 6. Duplicated logic between the two binaries
Keyword search, time-string parsing, event naming, and column-index lookup are implemented twice (`cmd/` and `cmd/scan/`). Extract into `pkg/` (e.g. `pkg/binlog`, `pkg/search`) so fixes land in one place.

## P2 — Dependencies & security

### 7. Outdated core dependency
`github.com/go-mysql-org/go-mysql` is at **v1.9.1**; latest is **v1.15.0** (bug fixes, newer MySQL 8.x event handling). Upgrade and re-run the test suite against the sample binlogs in `tests/`.

### 8. No vulnerability scanning
Add `govulncheck ./...` to CI and enable Dependabot (or Renovate) for Go modules and GitHub Actions.

## P3 — CI/CD & release engineering

### 9. CI Go version mismatch
`.github/workflows/build.yml` pins Go **1.21**, but `go.mod` declares `go 1.23.2`. Align CI with go.mod (use `go-version-file: go.mod`).

### 10. CI is build-only
Add steps: `gofmt` check, `go vet`, `staticcheck`, `govulncheck`, and run tests *before* building artifacts (tests currently run last). Add module/build caching.

### 11. No releases or versioning
No git tags, no release workflow, no `-version` flag in the binaries. Recommend: goreleaser (or a release workflow) producing linux/darwin × amd64/arm64 binaries on tag push, plus an embedded version via `-ldflags "-X main.version=..."`. Current CI only builds amd64 — no arm64 (Apple Silicon / Graviton).

## P4 — Repo hygiene & docs

### 12. Missing LICENSE file
README says "MIT License. See LICENSE file for details." — there is no LICENSE file. Add one (legally required for the stated license to apply).

### 13. Development-artifact docs in repo root
`PLAN.md`, `COMPLETION_SUMMARY.md`, `TASKS_COMPLETION_2026-05-05.md` are internal work logs, and a local `tags` ctags file sits in the tree (ignored, but stale). Move the useful content into `docs/` or delete; keep the root to README + LICENSE + Makefile.

### 14. Makefile gaps
No `test`, `fmt`, `lint`, or `vet` targets; `install` is an alias for `build` and doesn't install anywhere. Add standard targets so CI and humans run the same commands.

### 15. README accuracy pass
After the above: document exit codes, the `-version` flag, actual install instructions (`go install github.com/ChaosHour/go-parse/cmd@latest` won't produce a binary named `go-parse` — worth restructuring to `cmd/go-parse/` and `cmd/go-parse-scan/`), and remove/repoint the license claim until LICENSE exists.

## P5 — Nice to have (not blocking)

- **Structured logging / quiet mode:** progress output is hardcoded to 1-second ticks on stderr; add `-quiet`.
- **Context & signal handling:** long scans can't be cleanly interrupted; wire `signal.NotifyContext` through the parse loops.
- **Test coverage:** tests exist and use real sample binlogs (good), but coverage of `processFileAggregate` edge cases (UPDATE before/after images, NULL handling, since/until) and the schema parser's quoting variants is thin.
- **Binary layout:** `cmd/` → `cmd/go-parse/` so both binaries follow the standard Go layout.

---

## Suggested order of attack

1. **P0 #1–#3** — correctness first (small, isolated diffs)
2. **P1 #4–#5** — gofmt + staticcheck fix (mechanical)
3. **P3 #9–#10** — CI hardening so everything after is gated
4. **P2 #7–#8** — dependency upgrade + vuln scanning
5. **P3 #11** — release workflow & versioning
6. **P4 #12–#15** — LICENSE, docs cleanup, Makefile
7. **P1 #6, P5** — refactors last, once CI protects behavior
