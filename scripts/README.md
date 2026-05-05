# Scripts Directory

This directory contains helper scripts for working with MySQL binary logs
using the go-parse tools.

## Available Scripts

### scan_binlogs.sh

Basic script for scanning binlog files with go-parse-scan.

**Usage:**

```bash
./scripts/scan_binlogs.sh
```

**Features:**

- Scans binlog files in a directory
- Detects large operations
- Basic aggregation

---

### analyze_cloudsql_binlog.sh

Script for analyzing Cloud SQL binary log files with comprehensive options.

**Usage:**

```bash
./scripts/analyze_cloudsql_binlog.sh
```

**Features:**

- Cloud SQL specific analysis
- Schema validation
- Column extraction
- Aggregation by time periods
- Statistical summaries

**Configuration:**

Edit the script to set:

- `BINLOG_DIR` - Directory containing binlog files
- `SCHEMA_FILE` - Path to schema SQL file
- `SCHEMA_NAME` - Database name
- `TABLE_NAME` - Table to analyze
- Detection thresholds and filters

---

### demonstrate_fuzzy_search.sh

Demonstration script showing fuzzy search capabilities across binlog files.

**Usage:**

```bash
./scripts/demonstrate_fuzzy_search.sh
```

**Features:**

- SQL keyword fuzzy matching
- Context display around matches
- Case-insensitive search
- Match limiting
- Multiple keyword support

**Example Output:**

- Shows queries containing specific keywords (INSERT, UPDATE, DELETE, etc.)
- Displays context around matching keywords
- Provides match statistics

---

## Prerequisites

All scripts require:

- `go-parse-scan` binary built and available in `../bin/` or system PATH
- Binlog files to analyze
- (Optional) Schema SQL file for schema-aware operations

## Building the Tools

Before running scripts, build the binaries:

```bash
cd ..
make build
```

This creates:

- `bin/go-parse` - Main parser
- `bin/go-parse-scan` - Batch scanner (used by these scripts)

## Customization

Each script can be customized by editing the configuration variables at
the top of the file:

```bash
# Example variables you can modify:
BINLOG_DIR="tests"
SCHEMA_FILE="schema/sbtest-schema-only.sql"
THRESHOLD=1000
PARALLEL=4
```

## Common Options

Scripts typically support these patterns:

- `-scanDir` - Directory containing binlog files
- `-detectLarge` - Row threshold for "large" operations
- `-parallel` - Number of concurrent workers
- `-schemaFile` - Schema SQL file for table definitions
- `-aggregate` - Enable time-based aggregation
- `-fuzzySearch` - Enable keyword searching

See individual scripts for specific options used.

## Tips

1. **Start small**: Test with a single binlog file first
2. **Use schemas**: Schema files enable column extraction and validation
3. **Adjust thresholds**: Tune detection thresholds based on your workload
4. **Parallel processing**: Increase `-parallel` for large binlog sets
5. **Pretty output**: Add `-prettyJson` for readable JSON output

## Example Workflows

### Find Large Operations

```bash
cd scripts
./scan_binlogs.sh > large_ops.json
```

### Search for Specific SQL Patterns

```bash
cd scripts
./demonstrate_fuzzy_search.sh | grep -i "DELETE"
```

### Analyze with Schema

```bash
cd scripts
# Edit analyze_cloudsql_binlog.sh to set your schema file
./analyze_cloudsql_binlog.sh > analysis.json
```

## Troubleshooting

**Script not found:**

```bash
chmod +x scripts/*.sh
```

**Binary not found:**

```bash
cd .. && make build
```

**No binlog files found:**

- Check `BINLOG_DIR` setting in script
- Ensure binlog files match expected patterns (mysql-bin*, binlog*, etc.)
- Use `-binlogPattern` flag for custom naming

## Additional Resources

- Main README: `../README.md`
- Project plan: `../PLAN.md`
- Tool documentation: Run `./bin/go-parse-scan -h`
