package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChaosHour/go-parse/pkg/schema"
	"github.com/go-mysql-org/go-mysql/replication"
)

func getColumnNames(columns []schema.Column) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}

type LargeEvent struct {
	File      string                `json:"file"`
	Position  uint32                `json:"position"`
	EventType replication.EventType `json:"event_type"`
	EventName string                `json:"event_name"`
	Schema    string                `json:"schema,omitempty"`
	Table     string                `json:"table,omitempty"`
	Rows      int                   `json:"rows"`
	Query     string                `json:"query,omitempty"`
	Timestamp uint32                `json:"timestamp"`
}

// Enhanced aggregation record with dynamic column extraction
type AggregationRecord struct {
	Category      string         `json:"category"`
	Minute        string         `json:"minute"`
	Date          string         `json:"date"`
	Count         int            `json:"count"`
	FilterID      string         `json:"filter_id,omitempty"`
	ExtractedCols map[string]any `json:"extracted_cols,omitempty"`
	firstRow      rowRef         // earliest row seen; source of ExtractedCols
}

// rowRef identifies a row's position in the total order of scanned rows,
// so aggregation results are deterministic regardless of goroutine
// scheduling under -parallel.
type rowRef struct {
	time time.Time
	file string
	pos  uint32
	row  int
}

func (a rowRef) before(b rowRef) bool {
	if !a.time.Equal(b.time) {
		return a.time.Before(b.time)
	}
	if a.file != b.file {
		return a.file < b.file
	}
	if a.pos != b.pos {
		return a.pos < b.pos
	}
	return a.row < b.row
}

// Fuzzy search result structure
type FuzzySearchResult struct {
	Keyword string       `json:"keyword"`
	Count   int          `json:"count"`
	Matches []FuzzyMatch `json:"matches,omitempty"`
}

type FuzzyMatch struct {
	File       string `json:"file"`
	Position   uint32 `json:"position"`
	Timestamp  uint32 `json:"timestamp"`
	Query      string `json:"query"`
	Context    string `json:"context,omitempty"`
	LineNumber int    `json:"line_number,omitempty"`
}

// Fuzzy search statistics
type FuzzySearchStats struct {
	TotalFiles     int                           `json:"total_files"`
	TotalEvents    int64                         `json:"total_events"`
	Keywords       map[string]*FuzzySearchResult `json:"keywords"`
	ProcessingTime time.Duration                 `json:"processing_time"`
}

// AggregateConfig holds configuration for aggregate processing
type AggregateConfig struct {
	Threshold      int
	SchemaFile     string
	SchemaName     string
	TableName      string
	FilterCol      string
	FilterVal      string
	TimeCol        string
	CategoryCol    string
	ExtractCols    []string
	IncludeNulls   bool
	SampleSize     int
	ValidateSchema bool
	Since          time.Time
	Until          time.Time
	UseSince       bool
	UseUntil       bool
}

// progress counters
var (
	filesTotal            int64
	filesDone             int64
	eventsProcessedGlobal int64
	parseFailures         int64
	currentFile           atomic.Value // string
)

// exitOnParseFailures exits non-zero if any file failed to parse, so
// callers (scripts, cron) can detect partial results.
func exitOnParseFailures() {
	if n := atomic.LoadInt64(&parseFailures); n > 0 {
		fmt.Fprintf(os.Stderr, "completed with %d file parse failure(s)\n", n)
		os.Exit(1)
	}
}

// Add new flags for enhanced functionality
var (
	extractCols    = flag.String("extractCols", "", "additional columns to extract (comma-separated); values come from the earliest row in each category/minute group")
	includeNulls   = flag.Bool("includeNulls", false, "include records with NULL values in output")
	showStats      = flag.Bool("showStats", false, "show statistical summary after processing")
	sampleSize     = flag.Int("sampleSize", 0, "limit output to N records per category (0 = no limit)")
	validateSchema = flag.Bool("validateSchema", false, "validate schema and column existence before processing")
	listColumns    = flag.Bool("listColumns", false, "list all available columns in the specified table and exit")
	prettyJson     = flag.Bool("prettyJson", false, "output JSON in pretty-printed format")
	autoDiscover   = flag.Bool("autoDiscover", false, "automatically discover schema from DDL statements in binlogs")
	binlogPattern  = flag.String("binlogPattern", "", "pattern to match binlog files (empty = match common patterns like mysql-bin*, binlog*)")
	// Fuzzy search flags
	fuzzySearch     = flag.Bool("fuzzySearch", false, "enable fuzzy search for SQL keywords")
	searchKeywords  = flag.String("searchKeywords", "select,insert,update,delete,alter,drop", "comma-separated list of SQL keywords to search for")
	caseInsensitive = flag.Bool("caseInsensitive", true, "perform case-insensitive keyword search")
	showMatches     = flag.Bool("showMatches", false, "show matching statements with context")
	maxMatches      = flag.Int("maxMatches", 100, "maximum number of matches to display")
)

func createEncoder() *json.Encoder {
	enc := json.NewEncoder(os.Stdout)
	if *prettyJson {
		enc.SetIndent("", "  ")
	}
	return enc
}

func eventName(et replication.EventType) string {
	switch et {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return "WRITE_ROWS"
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return "UPDATE_ROWS"
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return "DELETE_ROWS"
	case replication.QUERY_EVENT:
		return "QUERY"
	default:
		return fmt.Sprintf("TYPE_%d", et)
	}
}

// isBinlogFile checks if a filename matches binlog patterns
func isBinlogFile(filename string, customPattern string) bool {
	if customPattern != "" {
		// Use custom pattern - simple substring match for now
		// Could be enhanced to support glob patterns or regex
		return strings.Contains(filename, customPattern)
	}

	// Default patterns for common binlog file naming conventions
	base := filepath.Base(filename)
	return strings.HasPrefix(base, "mysql-bin") ||
		strings.HasPrefix(base, "binlog") ||
		strings.HasPrefix(base, "mariadb-bin") ||
		strings.HasPrefix(base, "relay-log") ||
		strings.Contains(base, ".bin") && !strings.HasSuffix(base, ".bin.gz")
}

// autoDiscoverSchema scans binlog files for DDL statements and builds schema registry
func autoDiscoverSchema(binfiles []string) (*schema.SchemaRegistry, error) {
	sr := schema.NewSchemaRegistry()
	var ddlStatements []string

	fmt.Fprintf(os.Stderr, "Auto-discovering schema from %d binlog files...\n", len(binfiles))

	for _, binfile := range binfiles {
		p := replication.NewBinlogParser()
		err := p.ParseFile(binfile, 4, func(e *replication.BinlogEvent) error {
			if q, ok := e.Event.(*replication.QueryEvent); ok {
				query := string(q.Query)
				queryUpper := strings.ToUpper(strings.TrimSpace(query))

				// Capture CREATE TABLE and ALTER TABLE statements
				if strings.HasPrefix(queryUpper, "CREATE TABLE") ||
					strings.HasPrefix(queryUpper, "ALTER TABLE") ||
					strings.HasPrefix(queryUpper, "USE ") {
					ddlStatements = append(ddlStatements, query)
				}
			}
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("error parsing %s for DDL: %v", binfile, err)
		}
	}

	fmt.Fprintf(os.Stderr, "Found %d DDL statements\n", len(ddlStatements))

	if len(ddlStatements) > 0 {
		if err := sr.LoadFromDDL(ddlStatements); err != nil {
			return nil, fmt.Errorf("error loading DDL: %v", err)
		}

		// Print summary of discovered schema
		dbCount := len(sr.Databases)
		tableCount := 0
		for _, db := range sr.Databases {
			tableCount += len(db.Tables)
		}
		fmt.Fprintf(os.Stderr, "Discovered %d databases and %d tables\n", dbCount, tableCount)
	}

	return sr, nil
}

func processFile(binfile string, threshold int, out chan<- LargeEvent) error {
	p := replication.NewBinlogParser()
	var lastQuery string
	return p.ParseFile(binfile, 4, func(e *replication.BinlogEvent) error {
		// increment global event counter
		atomic.AddInt64(&eventsProcessedGlobal, 1)
		// capture QueryEvent text
		if q, ok := e.Event.(*replication.QueryEvent); ok {
			lastQuery = string(q.Query)
		}

		if rows, ok := e.Event.(*replication.RowsEvent); ok {
			schema := string(rows.Table.Schema)
			table := string(rows.Table.Table)
			rc := len(rows.Rows)
			if e.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
				rc = rc / 2
			}
			if threshold > 0 && rc >= threshold {
				out <- LargeEvent{
					File:      binfile,
					Position:  e.Header.LogPos,
					EventType: e.Header.EventType,
					EventName: eventName(e.Header.EventType),
					Schema:    schema,
					Table:     table,
					Rows:      rc,
					Query:     lastQuery,
					Timestamp: e.Header.Timestamp,
				}
			}
		}
		return nil
	})
}

// processFileAggregate parses binlog and aggregates table inserts by minute with enhanced metadata
func processFileAggregate(binfile string, cfg AggregateConfig, agg map[string]map[string]*AggregationRecord, aggMutex *sync.Mutex, preloadedSchema *schema.SchemaRegistry) error {
	// Load schema registry
	var sr *schema.SchemaRegistry
	if preloadedSchema != nil {
		sr = preloadedSchema
	} else {
		sr = schema.NewSchemaRegistry()
		if cfg.SchemaFile != "" {
			if err := sr.LoadFromFile(cfg.SchemaFile); err != nil {
				return fmt.Errorf("failed to load schema file: %v", err)
			}
		}
	}

	p := replication.NewBinlogParser()
	return p.ParseFile(binfile, 4, func(e *replication.BinlogEvent) error {
		if rows, ok := e.Event.(*replication.RowsEvent); ok {
			schemaName := string(rows.Table.Schema)
			tableName := string(rows.Table.Table)
			// match configured table
			fullName := schemaName + "." + tableName
			target := cfg.SchemaName + "." + cfg.TableName
			if cfg.TableName != "" && fullName != target {
				return nil
			}

			// Get table info from schema registry
			tbl := sr.GetTableInfo(schemaName, tableName)
			// column index lookup is case-insensitive; schema stores lowercase names
			var colIndex = func(col string) int {
				if tbl == nil {
					return -1
				}
				colKey := strings.ToLower(col)
				for i, c := range tbl.Columns {
					if strings.ToLower(c.Name) == colKey {
						return i
					}
				}
				return -1
			}

			pubIdx := colIndex(cfg.FilterCol)
			timeIdx := colIndex(cfg.TimeCol)
			categoryIdx := colIndex(cfg.CategoryCol)

			// Get indices for additional columns to extract
			extractIndices := make(map[string]int)
			for _, col := range cfg.ExtractCols {
				if idx := colIndex(col); idx >= 0 {
					extractIndices[col] = idx
				}
			}

			// iterate rows - handle UPDATE events which have before/after images
			for i, rowSlice := range rows.Rows {
				// For UPDATE events, skip the "before" image and only process "after" image
				if e.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
					if i%2 == 0 {
						continue // Skip "before" image
					}
				}

				// increment global event counter for progress
				atomic.AddInt64(&eventsProcessedGlobal, 1)

				// check publisher filter
				if pubIdx >= 0 && pubIdx < len(rowSlice) {
					val := fmt.Sprintf("%v", rowSlice[pubIdx])
					if val != cfg.FilterVal {
						continue
					}
				}

				// get time value
				var t time.Time
				if timeIdx >= 0 && timeIdx < len(rowSlice) {
					raw := rowSlice[timeIdx]
					switch v := raw.(type) {
					case time.Time:
						t = v
					case []byte:
						s := string(v)
						// try common MySQL datetime formats
						if parsed, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
							t = parsed
						} else if parsed, err := time.Parse("2006-01-02", s); err == nil {
							t = parsed
						} else if parsed, err := time.Parse(time.RFC3339, s); err == nil {
							t = parsed
						} else {
							// fallback to event header timestamp
							t = time.Unix(int64(e.Header.Timestamp), 0)
						}
					default:
						t = time.Unix(int64(e.Header.Timestamp), 0)
					}
				} else {
					t = time.Unix(int64(e.Header.Timestamp), 0)
				}

				if cfg.UseSince && t.Before(cfg.Since) {
					continue
				}
				if cfg.UseUntil && t.After(cfg.Until) {
					continue
				}

				// get category
				var category string
				if categoryIdx >= 0 && categoryIdx < len(rowSlice) {
					// coerce numeric types to string and trim
					raw := rowSlice[categoryIdx]
					switch v := raw.(type) {
					case []byte:
						category = strings.TrimSpace(string(v))
					default:
						category = strings.TrimSpace(fmt.Sprintf("%v", v))
					}
					if category == "" || category == "<nil>" {
						if cfg.IncludeNulls {
							category = "NULL"
						} else {
							category = "<unknown>"
						}
					}
				} else {
					if cfg.IncludeNulls {
						category = "NULL"
					} else {
						category = "<unknown>"
					}
				}

				minute := t.UTC().Format("20060102-1504")

				// Extract additional column values
				extractedValues := make(map[string]string)
				for colName, idx := range extractIndices {
					if idx >= 0 && idx < len(rowSlice) {
						raw := rowSlice[idx]
						switch v := raw.(type) {
						case []byte:
							extractedValues[colName] = strings.TrimSpace(string(v))
						case nil:
							extractedValues[colName] = "NULL"
						default:
							extractedValues[colName] = strings.TrimSpace(fmt.Sprintf("%v", v))
						}
					}
				}

				ref := rowRef{time: t, file: binfile, pos: e.Header.LogPos, row: i}

				aggMutex.Lock()
				if _, ok := agg[category]; !ok {
					agg[category] = make(map[string]*AggregationRecord)
				}
				if record, exists := agg[category][minute]; exists {
					record.Count++
					// ExtractedCols always reflects the earliest row, so
					// results don't depend on file processing order.
					if ref.before(record.firstRow) {
						record.firstRow = ref
						record.ExtractedCols = make(map[string]any, len(extractedValues))
						for colName, val := range extractedValues {
							record.ExtractedCols[colName] = val
						}
					}
				} else {
					// Create new record with extracted values
					record := &AggregationRecord{
						Category:      category,
						Minute:        minute,
						Count:         1,
						FilterID:      cfg.FilterVal,
						ExtractedCols: make(map[string]any),
						firstRow:      ref,
					}

					// Add extracted column values to the dynamic map
					for colName, val := range extractedValues {
						record.ExtractedCols[colName] = val
					}

					agg[category][minute] = record
				}
				aggMutex.Unlock()
			}
		}
		return nil
	})
}
func processFileFuzzySearch(binfile string, keywords []string, caseInsensitive bool, maxMatches int, results map[string]*FuzzySearchResult, resultsMutex *sync.Mutex, globalMatchCount *int64) error {
	p := replication.NewBinlogParser()

	return p.ParseFile(binfile, 4, func(e *replication.BinlogEvent) error {
		atomic.AddInt64(&eventsProcessedGlobal, 1)

		// Only process QueryEvents for fuzzy search
		if q, ok := e.Event.(*replication.QueryEvent); ok {
			query := string(q.Query)

			// Skip empty queries
			if strings.TrimSpace(query) == "" {
				return nil
			}

			// Search for keywords in the query
			for _, keyword := range keywords {
				searchTerm := keyword
				searchQuery := query

				if caseInsensitive {
					searchTerm = strings.ToLower(keyword)
					searchQuery = strings.ToLower(query)
				}

				// Check if keyword appears in query
				if strings.Contains(searchQuery, searchTerm) {
					resultsMutex.Lock()

					// Initialize result if not exists
					if results[keyword] == nil {
						results[keyword] = &FuzzySearchResult{
							Keyword: keyword,
							Count:   0,
							Matches: make([]FuzzyMatch, 0),
						}
					}

					// Increment count
					results[keyword].Count++

					// Add match details if showing matches and under global limit
					if *showMatches && atomic.LoadInt64(globalMatchCount) < int64(maxMatches) {
						// Extract context around the keyword
						context := extractContext(searchQuery, searchTerm, 100)

						match := FuzzyMatch{
							File:      binfile,
							Position:  e.Header.LogPos,
							Timestamp: e.Header.Timestamp,
							Query:     query,
							Context:   context,
						}

						results[keyword].Matches = append(results[keyword].Matches, match)
						atomic.AddInt64(globalMatchCount, 1)
					}

					resultsMutex.Unlock()
				}
			}
		}

		return nil
	})
}

// extractContext extracts context around a keyword in a query string
func extractContext(query, keyword string, contextLength int) string {
	keywordIndex := strings.Index(query, keyword)
	if keywordIndex == -1 {
		return query
	}

	start := max(keywordIndex-contextLength/2, 0)
	end := min(keywordIndex+len(keyword)+contextLength/2, len(query))

	context := query[start:end]

	// Add ellipsis if truncated
	if start > 0 {
		context = "..." + context
	}
	if end < len(query) {
		context = context + "..."
	}

	return context
}

// truncateString truncates a string to the specified length with ellipsis
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen < 3 {
		// If maxLen is too small for "...", just truncate
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "A batch scanner for MySQL binary log files with aggregation and analysis features.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -scanDir /path/to/binlogs -detectLarge 1000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -aggregate -schemaFile schema.sql -extractCols 'user_id,ip_address'\n", os.Args[0])
	}
	scanDir := flag.String("scanDir", "tests", "directory to scan for binlog files")
	threshold := flag.Int("detectLarge", 1000, "row threshold to consider an event large")
	parallel := flag.Int("parallel", 4, "number of concurrent parsers")
	aggregate := flag.Bool("aggregate", false, "aggregate table inserts by minute (JSON output)")
	schemaFile := flag.String("schemaFile", "", "schema SQL file to load for column mapping")
	schemaName := flag.String("schemaName", "", "database/schema name to filter (e.g. your_database)")
	tableName := flag.String("tableName", "", "table name to filter (e.g. your_table)")
	filterCol := flag.String("filterCol", "", "column name for record filtering")
	filterVal := flag.String("filterVal", "", "value to filter records by (string match)")
	timeCol := flag.String("timeCol", "", "time column name to use for grouping")
	categoryCol := flag.String("categoryCol", "", "column name that holds category id")
	sinceStr := flag.String("since", "", "only include events on/after this timestamp (RFC3339 or '2006-01-02 15:04')")
	untilStr := flag.String("until", "", "only include events before this timestamp (RFC3339 or '2006-01-02 15:04')")
	flag.Parse()

	// Show help if no arguments provided
	if flag.NFlag() == 0 && flag.NArg() == 0 {
		flag.Usage()
		os.Exit(0)
	}

	// gather files early if autoDiscover is enabled
	var files []string
	if *autoDiscover {
		files = make([]string, 0)
		err := filepath.WalkDir(*scanDir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				return nil
			}
			if isBinlogFile(path, *binlogPattern) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "error listing files: %v\n", err)
			os.Exit(1)
		}
		if len(files) == 0 {
			fmt.Fprintf(os.Stderr, "no binlog files found in %s\n", *scanDir)
			os.Exit(0)
		}
	}

	// Handle schema operations (listColumns or validateSchema) before file processing
	var validatedSchema *schema.SchemaRegistry
	if *listColumns || *validateSchema {
		fmt.Fprintf(os.Stderr, "Validating schema and column configuration...\n")
		sr := schema.NewSchemaRegistry()

		if *autoDiscover {
			// Auto-discover schema from binlog files
			fmt.Fprintf(os.Stderr, "Using auto-discovery mode...\n")
			discoveredSchema, err := autoDiscoverSchema(files)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Schema auto-discovery failed: %v\n", err)
				os.Exit(1)
			}
			sr = discoveredSchema
		} else if *schemaFile != "" {
			fmt.Fprintf(os.Stderr, "Loading schema file: %s\n", *schemaFile)
			if err := sr.LoadFromFile(*schemaFile); err != nil {
				fmt.Fprintf(os.Stderr, "Schema validation failed: %v\n", err)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "Schema file loaded successfully\n")
		} else {
			fmt.Fprintf(os.Stderr, "Error: schemaFile is required for -listColumns or -validateSchema (or use -autoDiscover)\n")
			os.Exit(1)
		}

		// Validate that schemaName and tableName are provided
		if *schemaName == "" || *tableName == "" {
			fmt.Fprintf(os.Stderr, "Error: -schemaName and -tableName are required for -listColumns or -validateSchema\n")
			fmt.Fprintf(os.Stderr, "\nAvailable schemas:\n")
			for dbName := range sr.Databases {
				fmt.Fprintf(os.Stderr, "  - %s\n", dbName)
			}
			os.Exit(1)
		}

		// Get table info
		fmt.Fprintf(os.Stderr, "Looking for table: %s.%s\n", *schemaName, *tableName)
		tbl := sr.GetTableInfo(*schemaName, *tableName)
		if tbl == nil {
			fmt.Fprintf(os.Stderr, "Schema validation failed: table '%s.%s' not found in schema\n", *schemaName, *tableName)

			// Show available tables in the requested schema
			if db, exists := sr.Databases[*schemaName]; exists {
				fmt.Fprintf(os.Stderr, "\nAvailable tables in schema '%s':\n", *schemaName)
				for tableName := range db.Tables {
					fmt.Fprintf(os.Stderr, "  - %s\n", tableName)
				}
			} else {
				fmt.Fprintf(os.Stderr, "\nSchema '%s' not found. Available schemas:\n", *schemaName)
				for dbName := range sr.Databases {
					fmt.Fprintf(os.Stderr, "  - %s\n", dbName)
				}
			}
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Table found with %d columns\n", len(tbl.Columns))

		// If listColumns flag is set, just list the columns and exit
		if *listColumns {
			fmt.Fprintf(os.Stderr, "\nAvailable columns in table '%s.%s':\n", *schemaName, *tableName)
			for _, col := range tbl.Columns {
				fmt.Fprintf(os.Stderr, "  - %s (%s)\n", col.Name, col.DataType)
			}
			fmt.Fprintf(os.Stderr, "\nTo extract all columns, use:\n")
			fmt.Fprintf(os.Stderr, "  -extractCols \"%s\"\n", strings.Join(getColumnNames(tbl.Columns), ","))
			fmt.Fprintf(os.Stderr, "\nNote: The -extractCols flag is used during actual processing (-aggregate),\n")
			fmt.Fprintf(os.Stderr, "      not with -listColumns. Don't include -extractCols when using -listColumns.\n")
			os.Exit(0)
		}

		// Perform validation if requested
		if *validateSchema {
			// Parse extract columns for validation
			var extractColsList []string
			if *extractCols != "" {
				extractColsList = strings.Split(*extractCols, ",")
				for i, col := range extractColsList {
					extractColsList[i] = strings.TrimSpace(col)
				}
			}

			// Check required columns
			requiredCols := []string{}
			if *filterCol != "" {
				requiredCols = append(requiredCols, *filterCol)
			}
			requiredCols = append(requiredCols, *timeCol, *categoryCol)
			for _, col := range extractColsList {
				if col != "" {
					requiredCols = append(requiredCols, col)
				}
			}

			missingCols := []string{}
			for _, col := range requiredCols {
				colKey := strings.ToLower(col)
				found := false
				for _, c := range tbl.Columns {
					if strings.ToLower(c.Name) == colKey {
						found = true
						break
					}
				}
				if !found {
					missingCols = append(missingCols, col)
				}
			}

			if len(missingCols) > 0 {
				fmt.Fprintf(os.Stderr, "Schema validation failed: missing columns in table '%s.%s': %v\n", *schemaName, *tableName, missingCols)
				fmt.Fprintf(os.Stderr, "\nAvailable columns in table '%s.%s':\n", *schemaName, *tableName)
				for _, col := range tbl.Columns {
					fmt.Fprintf(os.Stderr, "  - %s (%s)\n", col.Name, col.DataType)
				}
				fmt.Fprintf(os.Stderr, "\nUse -extractCols to specify additional columns to extract, e.g.:\n")
				fmt.Fprintf(os.Stderr, "  -extractCols \"%s\"\n", strings.Join(getColumnNames(tbl.Columns), ","))
				os.Exit(1)
			}

			fmt.Fprintf(os.Stderr, "Schema validation passed: table '%s.%s' found with %d columns\n", *schemaName, *tableName, len(tbl.Columns))
		}

		// Store the validated schema for processing
		validatedSchema = sr
	}

	// gather files (if not already done for autoDiscover)
	if files == nil {
		files = make([]string, 0)
		err := filepath.WalkDir(*scanDir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				return nil
			}
			if isBinlogFile(path, *binlogPattern) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "error listing files: %v\n", err)
			os.Exit(1)
		}

		if len(files) == 0 {
			fmt.Fprintf(os.Stderr, "no binlog files found in %s\n", *scanDir)
			os.Exit(0)
		}
	}
	if *aggregate {
		// parse since/until
		var since time.Time
		var until time.Time
		useSince := false
		useUntil := false
		parseTime := func(s string) (time.Time, error) {
			if s == "" {
				return time.Time{}, nil
			}
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				return t, nil
			}
			if t, err := time.Parse("2006-01-02 15:04", s); err == nil {
				return t, nil
			}
			if t, err := time.Parse("2006-01-02", s); err == nil {
				return t, nil
			}
			return time.Time{}, fmt.Errorf("unrecognized time format: %s", s)
		}
		if *sinceStr != "" {
			t, err := parseTime(*sinceStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bad --since: %v\n", err)
				os.Exit(1)
			}
			since = t
			useSince = true
		}
		if *untilStr != "" {
			t, err := parseTime(*untilStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bad --until: %v\n", err)
				os.Exit(1)
			}
			until = t
			useUntil = true
		}

		// Parse extract columns
		var extractColsList []string
		if *extractCols != "" {
			extractColsList = strings.Split(*extractCols, ",")
			for i, col := range extractColsList {
				extractColsList[i] = strings.TrimSpace(col)
			}
		}

		agg := make(map[string]map[string]*AggregationRecord)
		var aggMutex sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, *parallel)

		cfgType := AggregateConfig{
			Threshold:      *threshold,
			SchemaFile:     *schemaFile,
			SchemaName:     *schemaName,
			TableName:      *tableName,
			FilterCol:      *filterCol,
			FilterVal:      *filterVal,
			TimeCol:        *timeCol,
			CategoryCol:    *categoryCol,
			ExtractCols:    extractColsList,
			IncludeNulls:   *includeNulls,
			SampleSize:     *sampleSize,
			ValidateSchema: *validateSchema,
			Since:          since,
			Until:          until,
			UseSince:       useSince,
			UseUntil:       useUntil,
		}

		// gather files
		atomic.StoreInt64(&filesTotal, int64(len(files)))
		// start progress printer
		stopProgress := make(chan struct{})
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					cf := ""
					if v := currentFile.Load(); v != nil {
						cf = v.(string)
					}
					fmt.Fprintf(os.Stderr, "progress: files %d/%d, events %d, current=%s\n", atomic.LoadInt64(&filesDone), atomic.LoadInt64(&filesTotal), atomic.LoadInt64(&eventsProcessedGlobal), cf)
				case <-stopProgress:
					return
				}
			}
		}()

		for _, f := range files {
			wg.Add(1)
			sem <- struct{}{}
			go func(fn string) {
				defer wg.Done()
				defer func() { <-sem }()
				currentFile.Store(fn)
				if err := processFileAggregate(fn, cfgType, agg, &aggMutex, validatedSchema); err != nil {
					fmt.Fprintf(os.Stderr, "error parsing %s: %v\n", fn, err)
					atomic.AddInt64(&parseFailures, 1)
				}
				atomic.AddInt64(&filesDone, 1)
			}(f)
		}

		wg.Wait()
		close(stopProgress)

		// Emit JSON lines with enhanced metadata, sorted by category then
		// minute so output (and -sampleSize selection) is deterministic.
		enc := createEncoder()
		categories := make([]string, 0, len(agg))
		for category := range agg {
			categories = append(categories, category)
		}
		sort.Strings(categories)
		for _, category := range categories {
			minutes := agg[category]
			minuteKeys := make([]string, 0, len(minutes))
			for minute := range minutes {
				minuteKeys = append(minuteKeys, minute)
			}
			sort.Strings(minuteKeys)

			records := make([]*AggregationRecord, 0, len(minuteKeys))
			for _, minute := range minuteKeys {
				records = append(records, minutes[minute])
			}

			// Apply sampling limit - take first N records in minute order
			if cfgType.SampleSize > 0 && len(records) > cfgType.SampleSize {
				records = records[:cfgType.SampleSize]
			}

			for _, record := range records {
				// derive local date from the minute key (minute is UTC by construction)
				date := ""
				if tParsed, err := time.Parse("20060102-1504", record.Minute); err == nil {
					date = tParsed.UTC().In(time.Local).Format("2006-01-02")
				} else if len(record.Minute) >= 8 {
					// fallback: extract YYYYMMDD
					y := record.Minute[0:4]
					m := record.Minute[4:6]
					d := record.Minute[6:8]
					date = fmt.Sprintf("%s-%s-%s", y, m, d)
				}
				record.Date = date

				if err := enc.Encode(record); err != nil {
					fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
				}
			}
		}

		// Show statistics if requested
		if *showStats {
			fmt.Fprintf(os.Stderr, "\n=== Processing Statistics ===\n")
			totalRecords := 0
			categoryCounts := make(map[string]int)

			for category, minutes := range agg {
				categoryCount := 0
				for _, record := range minutes {
					categoryCount += record.Count
				}
				categoryCounts[category] = categoryCount
				totalRecords += categoryCount
			}

			fmt.Fprintf(os.Stderr, "Total records processed: %d\n", totalRecords)
			fmt.Fprintf(os.Stderr, "Categories found: %d\n", len(categoryCounts))
			fmt.Fprintf(os.Stderr, "Files processed: %d\n", len(files))

			// Show top categories
			fmt.Fprintf(os.Stderr, "\nTop categories by volume:\n")
			type categoryStat struct {
				category string
				count    int
			}
			var stats []categoryStat
			for cat, count := range categoryCounts {
				stats = append(stats, categoryStat{cat, count})
			}
			// Sort by count descending
			sort.Slice(stats, func(i, j int) bool {
				return stats[i].count > stats[j].count
			})
			for i, stat := range stats {
				if i >= 10 { // Show top 10
					break
				}
				fmt.Fprintf(os.Stderr, "  %s: %d records\n", stat.category, stat.count)
			}
		}

		exitOnParseFailures()
		return
	}

	// Handle fuzzy search mode
	if *fuzzySearch {
		fmt.Fprintf(os.Stderr, "Starting fuzzy search for SQL keywords...\n")

		// Parse search keywords
		keywords := strings.Split(*searchKeywords, ",")
		for i, keyword := range keywords {
			keywords[i] = strings.TrimSpace(keyword)
		}

		fmt.Fprintf(os.Stderr, "Searching for keywords: %v\n", keywords)
		fmt.Fprintf(os.Stderr, "Case insensitive: %t\n", *caseInsensitive)
		if *showMatches {
			fmt.Fprintf(os.Stderr, "Max matches to show: %d\n", *maxMatches)
		}

		// Initialize results
		fuzzyResults := make(map[string]*FuzzySearchResult)
		var resultsMutex sync.Mutex
		var globalMatchCount int64
		var wg sync.WaitGroup
		sem := make(chan struct{}, *parallel)

		// Progress tracking
		atomic.StoreInt64(&filesTotal, int64(len(files)))
		stopProgress := make(chan struct{})
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					cf := ""
					if v := currentFile.Load(); v != nil {
						cf = v.(string)
					}
					fmt.Fprintf(os.Stderr, "progress: files %d/%d, events %d, current=%s\n",
						atomic.LoadInt64(&filesDone), atomic.LoadInt64(&filesTotal),
						atomic.LoadInt64(&eventsProcessedGlobal), cf)
				case <-stopProgress:
					return
				}
			}
		}()

		// Process files concurrently
		for _, f := range files {
			wg.Add(1)
			sem <- struct{}{}
			go func(fn string) {
				defer wg.Done()
				defer func() { <-sem }()
				currentFile.Store(fn)
				if err := processFileFuzzySearch(fn, keywords, *caseInsensitive, *maxMatches, fuzzyResults, &resultsMutex, &globalMatchCount); err != nil {
					fmt.Fprintf(os.Stderr, "error parsing %s: %v\n", fn, err)
					atomic.AddInt64(&parseFailures, 1)
				}
				atomic.AddInt64(&filesDone, 1)
			}(f)
		}

		wg.Wait()
		close(stopProgress)

		// Output results
		enc := createEncoder()

		// Sort keywords by count for better output
		type keywordCount struct {
			keyword string
			count   int
		}
		var sortedKeywords []keywordCount
		for keyword, result := range fuzzyResults {
			sortedKeywords = append(sortedKeywords, keywordCount{keyword, result.Count})
		}

		// Sort by count descending
		sort.Slice(sortedKeywords, func(i, j int) bool {
			return sortedKeywords[i].count > sortedKeywords[j].count
		})

		// Output summary
		fmt.Fprintf(os.Stderr, "\n=== Fuzzy Search Results ===\n")
		fmt.Fprintf(os.Stderr, "Files processed: %d\n", len(files))
		fmt.Fprintf(os.Stderr, "Total events scanned: %d\n", atomic.LoadInt64(&eventsProcessedGlobal))

		totalMatches := 0
		for _, kc := range sortedKeywords {
			totalMatches += kc.count
		}
		fmt.Fprintf(os.Stderr, "Total keyword matches: %d\n", totalMatches)

		// Output detailed results
		for _, kc := range sortedKeywords {
			result := fuzzyResults[kc.keyword]

			// Output JSON result
			if err := enc.Encode(result); err != nil {
				fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
			}

			// Also print summary to stderr
			fmt.Fprintf(os.Stderr, "\nKeyword: %s\n", kc.keyword)
			fmt.Fprintf(os.Stderr, "Occurrences: %d\n", kc.count)
			if *showMatches && len(result.Matches) > 0 {
				fmt.Fprintf(os.Stderr, "Sample matches:\n")
				for i, match := range result.Matches {
					if i >= 5 { // Show only first 5 matches in summary
						fmt.Fprintf(os.Stderr, "  ... and %d more matches\n", len(result.Matches)-5)
						break
					}
					fmt.Fprintf(os.Stderr, "  %s:%d - %s\n",
						filepath.Base(match.File), match.Position,
						truncateString(match.Query, 80))
				}
			}
		}

		exitOnParseFailures()
		return
	}

	out := make(chan LargeEvent)
	var wg sync.WaitGroup
	sem := make(chan struct{}, *parallel)

	// printer
	printerDone := make(chan struct{})
	go func() {
		defer close(printerDone)
		enc := createEncoder()
		for ev := range out {
			if err := enc.Encode(ev); err != nil {
				fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
			}
		}
	}()

	atomic.StoreInt64(&filesTotal, int64(len(files)))
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cf := ""
				if v := currentFile.Load(); v != nil {
					cf = v.(string)
				}
				fmt.Fprintf(os.Stderr, "progress: files %d/%d, events %d, current=%s\n", atomic.LoadInt64(&filesDone), atomic.LoadInt64(&filesTotal), atomic.LoadInt64(&eventsProcessedGlobal), cf)
			case <-stopProgress:
				return
			}
		}
	}()

	for _, f := range files {
		wg.Add(1)
		sem <- struct{}{}
		go func(fn string) {
			defer wg.Done()
			defer func() { <-sem }()
			currentFile.Store(fn)
			if err := processFile(fn, *threshold, out); err != nil {
				fmt.Fprintf(os.Stderr, "error parsing %s: %v\n", fn, err)
				atomic.AddInt64(&parseFailures, 1)
			}
			atomic.AddInt64(&filesDone, 1)
		}(f)
	}

	wg.Wait()
	close(stopProgress)
	close(out)
	<-printerDone
	exitOnParseFailures()
}
