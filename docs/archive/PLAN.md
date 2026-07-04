# go-parse Fix Plan

Tracks issues found during code review, organized by priority.
Check off items as they are resolved.

---

## High — Correctness

### 1. Division by zero in `pkg/stats/stats.go`

- [x] **File:** `pkg/stats/stats.go` (~line 140)
- **Problem:** `float64(totalRows)/float64(totalOps)` panics/prints `NaN` when no
  rows events are processed (`totalOps == 0`).
- **Fix:** Wrap the average calculation in a `totalOps > 0` guard before printing.
- **Example:**

  ```go
  if totalOps > 0 {
      fmt.Printf("Average rows per operation: %.1f\n",
          float64(totalRows)/float64(totalOps))
  }
  ```

### 2. Debug print left in production code (`cmd/scan/main.go`)

- [x] **File:** `cmd/scan/main.go` — inside the `-validateSchema` block
- **Problem:** `fmt.Fprintf(os.Stderr, "Debug: requiredCols=%v, missingCols=%v,
  tableCols=%v\n", ...)` emits debug noise to stderr on every production run that
  uses `-validateSchema`.
- **Fix:** Remove the debug `Fprintf` line entirely.

### 3. Dead code in USE-statement parsing (`pkg/schema/schema.go`)

- [x] **File:** `pkg/schema/schema.go` — USE statement handler
- **Problem:** The first assignment to `dbName` is immediately overwritten and never
  used:

    ```go
      dbName := strings.Trim(strings.TrimPrefix(line, line[:4]), " ;`'")
      // ← dead assignment

---

## Medium — Quality / Maintainability

### 4. JSON encoder allocated per event in `cmd/main.go`

- [x] **File:** `cmd/main.go` — inside the `ParseFile` callback
- **Problem:** `json.NewEncoder(os.Stdout).Encode(...)` creates a new encoder
  on every INSERT/UPDATE/DELETE/fuzzy-match event. This allocates unnecessarily
  on every call.
- **Fix:** Create one `*json.Encoder` before the `ParseFile` call
    and close over it in the callback, matching the `createEncoder()` pattern
    already used in `cmd/scan/main.go`.

### 5. O(n²) bubble sorts in `cmd/scan/main.go`

- [x] **File:** `cmd/scan/main.go`
- **Problem:** Two hand-rolled bubble sorts exist — one for top-categories stats
  output, one for fuzzy keyword results. At scale with many tables/keywords the
  quadratic cost is noticeable.
- **Fix:** Replace both with `sort.Slice`.

  ```go
  sort.Slice(stats, func(i, j int) bool {
      return stats[i].count > stats[j].count
  })
  ```

### 6. Anonymous struct config prevents unit testing (`processFileAggregate`)

- [x] **File:** `cmd/scan/main.go`
- **Problem:** `processFileAggregate` takes an anonymous
    `struct { Threshold int; SchemaFile string; ... }` as its config parameter.
    Test files cannot reference this type without duplicating the struct definition.
- **Fix:** Promote the anonymous struct to a named package-level type, e.g.
  `AggregateConfig`.

### 7. `matchCount` per-file instead of global in `processFileFuzzySearch`

- [x] **File:** `cmd/scan/main.go` — `processFileFuzzySearch`
- **Problem:** `matchCount` is a local variable per goroutine, so the `-maxMatches`
  limit is applied per file, not globally. With N files a user gets up to
  `maxMatches × N` results.
- **Fix:** Pass `matchCount` as a shared `*int64` atomic counter,
    or enforce the cap when collecting results after `wg.Wait()`.

### 8. String matching on error messages for `stopAtNext`

- [x] **File:** `cmd/main.go`
- **Problem:** The `stopAtNext` flow is signaled by returning an error from the
  callback whose message starts with `"found next event"`. The caller checks:

  ```go
  if !strings.HasPrefix(err.Error(), "found next event") {
  ```

  This is fragile — any message change silently breaks the logic.
- **Fix:** Declare a package-level sentinel error and use `errors.Is`:

  ```go
  var errFoundNextEvent = errors.New("found next event")
  // in callback:
  return fmt.Errorf("%w at position %d", errFoundNextEvent, eventStartPos)
  // in caller:
  if !errors.Is(err, errFoundNextEvent) {
  ```

---

## Low — Style / Minor

### 9. Remove the `Print()` alias in `pkg/stats/stats.go`

- [x] **File:** `pkg/stats/stats.go`
- **Problem:** `Print()` is a thin wrapper around `PrintStats()` with a comment
  claiming "backward compatibility", but both methods exist only in this repo. One
  name is enough.
- **Fix:** Update the one call site in `cmd/main.go` to use `PrintStats()` directly
  and delete the `Print()` alias.

### 10. `autoDiscover` flag is unimplemented

- [x] **File:** `cmd/scan/main.go` and `README.md`
- **Problem:** The `-autoDiscover` flag is defined, gated in the schema validation
  block, and documented in the README, but no actual DDL-discovery logic exists.
- **Options (pick one):**
  - Implement the DDL auto-discovery feature.
  - Remove the flag from the code and the README until it is implemented.
- **Resolution:** Implemented the DDL auto-discovery feature. The tool now scans
  binlog files for CREATE TABLE, ALTER TABLE, and USE statements to build the
  schema registry automatically.

### 11. Code fence language for CLI flag blocks in README

- [x] **File:** `README.md`
- **Problem:** Both options blocks use ` ```Go ` as the fence language, but the
  content is command-line flag syntax, not Go code. Editors and renderers will
  attempt (incorrect) Go syntax highlighting.
- **Fix:** Change both to ` ```text ` (or remove the language tag entirely).

---

## Notes

- All existing tests pass (`go test ./...` clean).
- `go vet ./...` is clean.
- README is markdownlint-clean (0 errors).
- The UPDATE after-image bug (`getAfterImageRow`) is already fixed and tested.

---

## Next Tasks

- [x] Verify `go test ./...` after the current repository changes.
- [x] Add coverage or unit tests for `autoDiscoverSchema` and the
  `autoDiscover` workflow.
- [x] Harden fuzzy search global matching so `-maxMatches` cannot be
  overshot under concurrency.
- [x] Review `autoDiscover` file filtering and broaden it beyond
  `mysql-bin*` naming if needed.
- [x] Regenerate the `tags` file after source changes to keep code
  navigation accurate.
- [x] Document any behavior changes in `README.md` if new CLI semantics are introduced.

---

## Recent Improvements (2026-05-05)

### Test Coverage Added

- Added comprehensive unit tests for `cmd/scan/main_test.go`
- Tests for `autoDiscoverSchema()` with real binlog files
- Tests for fuzzy search global match limiting
- Tests for helper functions (`extractContext`, `getColumnNames`, `truncateString`)
- Tests for `AggregateConfig` struct (named type)
- All tests passing ✅

### Binlog File Pattern Matching Enhanced

- **New flag:** `-binlogPattern` for custom file matching
- Default patterns now support multiple naming conventions:
  - `mysql-bin*` (MySQL default)
  - `binlog*` (generic)
  - `mariadb-bin*` (MariaDB)
  - `relay-log*` (MySQL replication relay logs)
  - `*.bin` files (excluding compressed `.bin.gz`)
- Added `isBinlogFile()` helper function for consistent pattern matching
- No more hardcoded filename patterns

### Bug Fixes

- Fixed `truncateString()` panic when maxLen < 3
- Now handles edge case gracefully by omitting "..." suffix

### Fuzzy Search Improvements

- Global match counter already properly implemented with `atomic.Int64`
- Verified thread-safe under concurrent processing
- Test confirms `-maxMatches` limit is enforced globally, not per-file

### Documentation Updates

- Added `-binlogPattern` flag to README options
- Added new "Custom Binlog File Patterns" section with example
- Updated PLAN.md with completed tasks and improvements

---

## Summary

**All 11 original PLAN.md items complete + 6 additional "Next Tasks" complete!**

The codebase is now:

- Fully tested with comprehensive unit tests
- More flexible with custom binlog pattern matching
- Better documented with examples
- Ready for production use with various binlog naming conventions
