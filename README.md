# go-parse

A MySQL binary log parser utility with two CLI tools for different use cases.

## Tools

This repository contains two CLI tools:

### 1. go-parse - Main Binlog Parser
Primary tool for parsing MySQL binary logs with advanced features like JSON output, column extraction, and schema-aware parsing.

### 2. go-parse-scan - Batch Scanner
Specialized tool for scanning directories of binlog files to detect large operations and aggregate statistics.

## Quick Start

### Build Both Tools
```bash
make build
# or
make install
```

This creates:
- `bin/go-parse` - Main parser
- `bin/go-parse-scan` - Batch scanner

## go-parse Usage

### Basic Usage
```bash
./bin/go-parse -file <binlog_file> -all
```

### Command Line Options
```
-file string        Binlog file to parse (required)
-all                Parse entire binlog file
-offset int         Starting offset (use -1 to ignore) (default -1)
-logPosition int    Log position to start from (use -1 to ignore) (default -1)
-listPositions      List all log positions in the binlog
-stopAtNext         Stop at the next log position
-showStats          Show operation statistics by database and table
-verbose            Show detailed position information for each event
-schema string      MySQL schema dump file to load
-detectLarge int    Detect operations with at least N rows and print details
-timeCol string     Time column name to use for stats (default "created_at")
-json               Output events as structured JSON
-extractCols string Comma-separated list of columns to extract values from
-decodeRows         Decode and display actual row data (like mysqlbinlog -vv)
-fuzzySearch        Enable fuzzy search for SQL keywords
-searchKeywords string Comma-separated list of SQL keywords to search for (default "select,insert,update,delete,alter,drop")
-caseInsensitive    Perform case-insensitive keyword search (default true)
```

### Examples

#### List all log positions
```bash
./bin/go-parse -file tests/mysql-bin.000001 -listPositions
```

#### Parse specific position
```bash
./bin/go-parse -file tests/mysql-bin.000001 -logPosition 10093 -stopAtNext
```

#### Parse entire file with schema
```bash
./bin/go-parse -file tests/mysql-bin.000012 -schema schema/sbtest-schema-only.sql -all
```

#### JSON Output with Column Extraction
```bash
./bin/go-parse -file tests/mysql-bin.000012 -schema schema/sbtest-schema-only.sql -json -extractCols "id,k,c" -all
```

#### Statistics Mode
```bash
./bin/go-parse -file tests/mysql-bin.000012 -schema schema/sbtest-schema-only.sql -showStats -all
```

#### Fuzzy Search for SQL Keywords
```bash
./bin/go-parse -file tests/mysql-bin.000012 -fuzzySearch -searchKeywords "INSERT,UPDATE,DELETE" -all
```

## go-parse-scan Usage

### Basic Usage
```bash
./bin/go-parse-scan -scanDir <directory> -detectLarge <threshold>
```

### Command Line Options
```
-scanDir string       Directory to scan for binlog files (default "tests")
-detectLarge int      Row threshold to consider an event large (default 1000)
-parallel int         Number of concurrent parsers (default 4)
-aggregate           Aggregate table inserts by minute (JSON output)
-schemaFile string   Schema SQL file to load for column mapping
-schemaName string   Database/schema name to filter
-tableName string    Table name to filter
-filterCol string    Column name for record filtering
-filterVal string     Value to filter records by (string match)
-timeCol string      Time column name to use for grouping
-categoryCol string  Column name that holds category id
-extractCols string  Additional columns to extract (comma-separated)
-includeNulls       Include records with NULL values in output
-showStats          Show statistical summary after processing
-sampleSize int     Limit output to N records per category (0 = no limit)
-validateSchema     Validate schema and column existence before processing
-listColumns        List all available columns in the specified table and exit
-prettyJson         Output JSON in pretty-printed format
-autoDiscover       Automatically discover schema from DDL statements in binlogs
-fuzzySearch        Enable fuzzy search for SQL keywords
-searchKeywords     Comma-separated list of SQL keywords to search for (default "select,insert,update,delete,alter,drop")
-caseInsensitive    Perform case-insensitive keyword search (default true)
-showMatches        Show matching statements with context
-maxMatches int     Maximum number of matches to display (default 100)
-since string       Only include events on/after this timestamp
-until string       Only include events before this timestamp
```

### Examples

#### Scan for large operations
```bash
./bin/go-parse-scan -scanDir tests -detectLarge 10
```

#### Advanced Aggregation with Schema Validation
```bash
./bin/go-parse-scan \
  -scanDir tests \
  -aggregate \
  -schemaFile schema/sbtest-schema-only.sql \
  -schemaName sbtest \
  -tableName sbtest1 \
  -categoryCol k \
  -timeCol id \
  -extractCols "c,pad" \
  -validateSchema \
  -showStats \
  -parallel 8
```

#### List Available Columns
```bash
./bin/go-parse-scan \
  -schemaFile schema/sbtest-schema-only.sql \
  -schemaName sbtest \
  -tableName sbtest1 \
  -listColumns
```

#### Fuzzy Search Across Multiple Files
```bash
./bin/go-parse-scan \
  -scanDir tests \
  -fuzzySearch \
  -searchKeywords "INSERT,UPDATE,DELETE" \
  -showMatches \
  -maxMatches 20
```

## Schema File Format

The tools support MySQL schema dump files with CREATE TABLE statements:

```sql
CREATE TABLE `sbtest1` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `k` int(10) unsigned NOT NULL DEFAULT '0',
  `c` char(120) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  `pad` char(60) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `k_1` (`k`)
);
```

## Build Instructions

### Using Makefile (Recommended)
```bash
# Build both tools
make build

# Install (same as build)
make install

# Clean binaries
make clean
```

### Manual Build
```bash
# Build main parser
go build -o bin/go-parse ./cmd

# Build scanner
go build -o bin/go-parse-scan ./cmd/scan
```

## Dependencies

- go-mysql-org/go-mysql - MySQL replication protocol library

## License

MIT License. See LICENSE file for details.
