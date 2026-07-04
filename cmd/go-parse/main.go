package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ChaosHour/go-parse/pkg/schema"
	"github.com/ChaosHour/go-parse/pkg/stats" // Updated import path
	"github.com/ChaosHour/go-parse/pkg/version"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Sentinel error for stopAtNext flow
var errFoundNextEvent = errors.New("found next event")

// Add event type mapping
var mysqlTypeNames = map[byte]string{
	1:   "TINYINT",
	2:   "SMALLINT",
	3:   "INT",
	4:   "FLOAT",
	5:   "DOUBLE",
	6:   "NULL",
	7:   "TIMESTAMP",
	8:   "BIGINT",
	9:   "MEDIUMINT",
	10:  "DATE",
	11:  "TIME",
	12:  "DATETIME",
	13:  "YEAR",
	15:  "VARCHAR",
	16:  "BIT",
	17:  "TIMESTAMP2",
	18:  "DATETIME2",
	19:  "TIME2",
	245: "JSON",
	246: "DECIMAL",
	247: "ENUM",
	248: "SET",
	249: "TINY_BLOB",
	250: "MEDIUM_BLOB",
	251: "LONG_BLOB",
	252: "BLOB",
	253: "VAR_STRING",
	254: "STRING",
}

type EventRecord struct {
	EventType       string         `json:"event_type"`
	Timestamp       string         `json:"timestamp"`
	ThreadID        int64          `json:"thread_id,omitempty"`
	ServerID        uint32         `json:"server_id"`
	Schema          string         `json:"schema"`
	Table           string         `json:"table"`
	RowsAffected    int            `json:"rows_affected"`
	QueryType       string         `json:"query_type,omitempty"`
	ExtractedValues map[string]any `json:"extracted_values,omitempty"`
	TransactionID   string         `json:"transaction_id,omitempty"`
	Query           string         `json:"query,omitempty"`
}

// Helper function to extract thread_id from query status variables
func extractThreadId(query string) int64 {
	// Look for SET @@session.pseudo_thread_id=N in the query
	if strings.Contains(query, "pseudo_thread_id") {
		parts := strings.Split(query, "pseudo_thread_id=")
		if len(parts) > 1 {
			idStr := strings.Fields(parts[1])[0]
			idStr = strings.Trim(idStr, " ;")
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				return id
			}
		}
	}
	return 0
}

// Helper function to check if query contains search keywords
func queryContainsKeywords(query string, keywords []string, caseInsensitive bool) (bool, string) {
	if caseInsensitive {
		query = strings.ToLower(query)
	}

	for _, keyword := range keywords {
		searchTerm := keyword
		if caseInsensitive {
			searchTerm = strings.ToLower(keyword)
		}

		if strings.Contains(query, searchTerm) {
			return true, keyword
		}
	}
	return false, ""
}

// Helper function to decode and display row data
func decodeAndDisplayRowData(rowsEvent *replication.RowsEvent, tblInfo *schema.Table, operation string, schemaName, tableName string, e *replication.BinlogEvent) {
	fmt.Printf("\n### %s on %s.%s ###\n", operation, schemaName, tableName)
	fmt.Printf("Timestamp: %s\n", time.Unix(int64(e.Header.Timestamp), 0).Format("2006-01-02 15:04:05"))
	fmt.Printf("Log position: %d\n", e.Header.LogPos)
	fmt.Printf("Rows affected: %d\n", len(rowsEvent.Rows))

	// For UPDATE events, rows are in before/after pairs
	if operation == "UPDATE" {
		fmt.Printf("\nBEFORE IMAGES:\n")
		fmt.Printf("==============\n")
		for i := 0; i < len(rowsEvent.Rows); i += 2 {
			displayRowData(rowsEvent.Rows[i], tblInfo, i/2+1)
		}

		fmt.Printf("\nAFTER IMAGES:\n")
		fmt.Printf("=============\n")
		for i := 1; i < len(rowsEvent.Rows); i += 2 {
			displayRowData(rowsEvent.Rows[i], tblInfo, (i-1)/2+1)
		}
	} else {
		// INSERT or DELETE
		imageType := "INSERT"
		if operation == "DELETE" {
			imageType = "DELETE"
		}

		fmt.Printf("\n%s IMAGES:\n", imageType)
		fmt.Printf("%s=============\n", strings.Repeat("=", len(imageType)))
		for i, row := range rowsEvent.Rows {
			displayRowData(row, tblInfo, i+1)
		}
	}
	fmt.Println()
}

// Helper function to display individual row data
func displayRowData(row []any, tblInfo *schema.Table, rowNum int) {
	fmt.Printf("\n--- Row %d ---\n", rowNum)

	if tblInfo == nil {
		// No schema info, just show raw data
		for i, col := range row {
			fmt.Printf("  @%d=%v\n", i+1, formatColumnValue(col))
		}
	} else {
		// Use schema info for better formatting
		for i, col := range tblInfo.Columns {
			if i < len(row) {
				value := formatColumnValue(row[i])
				fmt.Printf("  %s=%s\n", col.Name, value)
			}
		}
	}
}

// Helper function to format column values nicely
func formatColumnValue(value any) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case []byte:
		// Try to interpret as string, but show as bytes if it contains non-printable chars
		str := string(v)
		if len(str) > 100 {
			return fmt.Sprintf("'%s...' (%d bytes)", str[:100], len(v))
		}
		// Check for non-printable characters
		for _, b := range v {
			if b < 32 && b != 9 && b != 10 && b != 13 { // Allow tab, newline, carriage return
				return fmt.Sprintf("<%d bytes>", len(v))
			}
		}
		return fmt.Sprintf("'%s'", str)
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Helper function to analyze query type
func analyzeQueryType(query string) string {
	query = strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(query, "CREATE") || strings.HasPrefix(query, "ALTER") || strings.HasPrefix(query, "DROP") {
		return "DDL"
	} else if strings.HasPrefix(query, "INSERT") || strings.HasPrefix(query, "UPDATE") || strings.HasPrefix(query, "DELETE") {
		return "DML"
	} else if strings.HasPrefix(query, "SELECT") {
		return "SELECT"
	} else if strings.HasPrefix(query, "BEGIN") || strings.HasPrefix(query, "COMMIT") || strings.HasPrefix(query, "ROLLBACK") {
		return "TRANSACTION"
	}
	return "OTHER"
}

// Helper function to extract column values from row
func extractColumnValues(rowSlice []any, tblInfo *schema.Table, extractCols []string) map[string]any {
	values := make(map[string]any)
	if tblInfo == nil || len(extractCols) == 0 {
		return values
	}

	for _, colName := range extractCols {
		colIdx := -1
		for i, c := range tblInfo.Columns {
			if strings.EqualFold(c.Name, colName) {
				colIdx = i
				break
			}
		}
		if colIdx >= 0 && colIdx < len(rowSlice) {
			raw := rowSlice[colIdx]
			// Convert to appropriate type
			switch v := raw.(type) {
			case []byte:
				values[colName] = string(v)
			case time.Time:
				values[colName] = v.Format("2006-01-02 15:04:05")
			default:
				values[colName] = v
			}
		}
	}
	return values
}

func getAfterImageRow(rows [][]any) []any {
	// UPDATE events store rows as before/after pairs. Prefer the first after-image.
	if len(rows) > 1 {
		return rows[1]
	}
	if len(rows) > 0 {
		return rows[0]
	}
	return nil
}

var (
	binlogFile     = flag.String("file", "", "Binlog file to parse")
	offset         = flag.Int64("offset", -1, "Starting offset (use -1 to ignore)")
	logPosition    = flag.Int64("logPosition", -1, "Log position to start from (use -1 to ignore)")
	listPositions  = flag.Bool("listPositions", false, "List all log positions in the binlog")
	stopAtNext     = flag.Bool("stopAtNext", false, "Stop at the next log position")
	showStats      = flag.Bool("showStats", false, "Show operation statistics by database and table")
	verbose        = flag.Bool("verbose", false, "Show detailed position information for each event")
	parseAll       = flag.Bool("all", false, "Parse entire binlog file")
	schemaFile     = flag.String("schema", "", "MySQL schema dump file to load")
	largeThreshold = flag.Int("detectLarge", 0, "Detect operations with at least N rows and print details")
	timeCol        = flag.String("timeCol", "time_written", "time column name to use for stats (if present in table)")
	jsonOutput     = flag.Bool("json", false, "Output events as structured JSON")
	extractCols    = flag.String("extractCols", "", "comma-separated list of columns to extract values from")
	decodeRows     = flag.Bool("decodeRows", false, "Decode and display actual row data (like mysqlbinlog -vv)")
	// Fuzzy search flags
	fuzzySearch     = flag.Bool("fuzzySearch", false, "enable fuzzy search for SQL keywords")
	searchKeywords  = flag.String("searchKeywords", "select,insert,update,delete,alter,drop", "comma-separated list of SQL keywords to search for")
	caseInsensitive = flag.Bool("caseInsensitive", true, "perform case-insensitive keyword search")
	showVersion     = flag.Bool("version", false, "print version and exit")
)

// Add this function for binlog validation
func isValidBinlogFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read magic header (4 bytes)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(file, magic); err != nil {
		return fmt.Errorf("failed to read magic header: %v", err)
	}

	// MySQL binlog magic number is 0xfe62696e
	expectedMagic := []byte{0xfe, 0x62, 0x69, 0x6e}
	if !bytes.Equal(magic, expectedMagic) {
		return fmt.Errorf("invalid binlog format: incorrect magic number")
	}

	return nil
}

// Add custom event dumper
func dumpTableMapEvent(e *replication.TableMapEvent) {
	fmt.Printf("=== TableMapEvent ===\n")
	fmt.Printf("Schema: %s\n", string(e.Schema))
	fmt.Printf("Table: %s\n", string(e.Table))
	fmt.Printf("Column count: %d\n", e.ColumnCount)

	fmt.Printf("\nColumns:\n")
	for i, t := range e.ColumnType {
		typeName := mysqlTypeNames[t]
		if typeName == "" {
			typeName = fmt.Sprintf("TYPE_%d", t)
		}

		nullable := "NO"
		if (e.NullBitmap[i/8]>>uint(i%8))&1 == 1 {
			nullable = "YES"
		}

		fmt.Printf("  [%d] %-12s nullable=%s\n", i, typeName, nullable)
	}
	fmt.Printf("\nTable ID: %d\n", e.TableID)
	fmt.Printf("Flags: %d\n", e.Flags)
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -file <binlog file> [-all] [-offset <offset>] [-logPosition <log position>] [-listPositions] [-stopAtNext] [-showStats] [-verbose] [-schema <schema file>] [-fuzzySearch] [-searchKeywords <keywords>] [-caseInsensitive]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("go-parse %s\n", version.String())
		return
	}

	if *binlogFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Enhance file validation
	if _, err := os.Stat(*binlogFile); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: Binlog file %s does not exist\n", *binlogFile)
		os.Exit(1)
	}

	// Add binlog format validation
	if err := isValidBinlogFile(*binlogFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s is not a valid MySQL binlog file: %v\n", *binlogFile, err)
		os.Exit(1)
	}

	if *listPositions {
		listAllLogPositions(*binlogFile)
		return
	}

	const BINLOG_START_POSITION = 4
	startPosition := int64(BINLOG_START_POSITION) // Default start position for parsing entire file
	if !*parseAll {
		startPosition = *offset
		if startPosition == -1 && *logPosition != -1 {
			startPosition = *logPosition
		}

		if startPosition == -1 {
			fmt.Fprintf(os.Stderr, "Error: Either offset, log position, or -all flag must be specified\n")
			flag.Usage()
			os.Exit(1)
		}
	}

	// Load schema if provided
	var schemaRegistry *schema.SchemaRegistry
	if *schemaFile != "" {
		schemaRegistry = schema.NewSchemaRegistry()
		if err := schemaRegistry.LoadFromFile(*schemaFile); err != nil {
			fmt.Fprintf(os.Stderr, "Error loading schema file: %v\n", err)
			os.Exit(1)
		}
		// Only print schema summary in verbose mode
		if *verbose {
			schemaRegistry.PrintSummary()
		}
	}

	// Create statistics collector
	statistics := stats.NewStatistics()

	// Parse fuzzy search keywords if enabled
	var searchKeywordsList []string
	if *fuzzySearch {
		if *searchKeywords != "" {
			searchKeywordsList = strings.Split(*searchKeywords, ",")
			for i, keyword := range searchKeywordsList {
				searchKeywordsList[i] = strings.TrimSpace(keyword)
			}
		}
		fmt.Fprintf(os.Stderr, "Fuzzy search enabled for keywords: %v (case-insensitive: %t)\n", searchKeywordsList, *caseInsensitive)
	}

	p := replication.NewBinlogParser()
	var eventsFound bool
	var lastQuery string
	var currentGTID string
	var extractColsList []string

	// Create JSON encoder once if JSON output is enabled
	var encoder *json.Encoder
	if *jsonOutput {
		encoder = json.NewEncoder(os.Stdout)
	}

	if *extractCols != "" {
		extractColsList = strings.Split(*extractCols, ",")
		for i, col := range extractColsList {
			extractColsList[i] = strings.TrimSpace(col)
		}
	}
	err := p.ParseFile(*binlogFile, startPosition, func(e *replication.BinlogEvent) error {
		// Calculate event positions
		eventStartPos := e.Header.LogPos - uint32(e.Header.EventSize)
		eventEndPos := e.Header.LogPos

		// Enhanced debug position info
		if *verbose {
			fmt.Printf("\nEvent boundaries:")
			fmt.Printf("\n  Start position: %d", eventStartPos)
			fmt.Printf("\n  Size: %d bytes", e.Header.EventSize)
			fmt.Printf("\n  End position: %d (where next event starts)", eventEndPos)
			if *parseAll {
				fmt.Printf("\n  (Reading entire binlog starting from position %d)", BINLOG_START_POSITION)
			} else {
				fmt.Printf("\n  (Target position: %d)", startPosition)
			}
			fmt.Println()
		}

		// Process events based on -all flag or specific position
		if *parseAll || eventStartPos == uint32(startPosition) {
			eventsFound = true

			// Record event type for statistics
			statistics.RecordEventType(byte(e.Header.EventType))

			// Capture GTID events
			if gtidEvent, ok := e.Event.(*replication.GTIDEvent); ok {
				if gtidSet, err := gtidEvent.GTIDNext(); err == nil {
					currentGTID = gtidSet.String()
				}
			}

			// Capture Query events
			if queryEvent, ok := e.Event.(*replication.QueryEvent); ok {
				lastQuery = string(queryEvent.Query)

				// Apply fuzzy search filtering if enabled
				if *fuzzySearch && len(searchKeywordsList) > 0 {
					matches, matchedKeyword := queryContainsKeywords(lastQuery, searchKeywordsList, *caseInsensitive)
					if matches {
						// Output matching query with full metadata
						eventTime := time.Unix(int64(e.Header.Timestamp), 0)

						// Add fuzzy search metadata
						if *jsonOutput {
							// Create a custom JSON structure for fuzzy search results
							fuzzyRecord := map[string]any{
								"event_type":      "FUZZY_SEARCH_MATCH",
								"timestamp":       eventTime.Format(time.RFC3339),
								"thread_id":       extractThreadId(lastQuery),
								"server_id":       e.Header.ServerID,
								"schema":          string(queryEvent.Schema),
								"query_type":      analyzeQueryType(lastQuery),
								"transaction_id":  currentGTID,
								"query":           lastQuery,
								"matched_keyword": matchedKeyword,
								"file":            *binlogFile,
								"log_position":    e.Header.LogPos,
								"event_size":      e.Header.EventSize,
							}

							if err := encoder.Encode(fuzzyRecord); err != nil {
								fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
							}
						} else {
							// Text output for fuzzy search matches
							fmt.Printf("\n=== FUZZY SEARCH MATCH ===\n")
							fmt.Printf("Keyword: %s\n", matchedKeyword)
							fmt.Printf("File: %s\n", *binlogFile)
							fmt.Printf("Position: %d\n", e.Header.LogPos)
							fmt.Printf("Timestamp: %s\n", eventTime.Format("2006-01-02 15:04:05"))
							fmt.Printf("Thread ID: %d\n", extractThreadId(lastQuery))
							fmt.Printf("Schema: %s\n", string(queryEvent.Schema))
							fmt.Printf("Query Type: %s\n", analyzeQueryType(lastQuery))
							if currentGTID != "" {
								fmt.Printf("GTID: %s\n", currentGTID)
							}
							fmt.Printf("Query: %s\n", lastQuery)
							fmt.Println()
						}
					}
				}
			}

			// Only dump event details if not showing stats and not in JSON mode
			if !*showStats && !*jsonOutput {
				// Replace default event dumping with custom formatting
				if tableMap, ok := e.Event.(*replication.TableMapEvent); ok {
					dumpTableMapEvent(tableMap)
				} else {
					e.Dump(os.Stdout)
				}
			}

			// Add statistics tracking for different event types
			if rowsEvent, ok := e.Event.(*replication.RowsEvent); ok {
				schemaName := string(rowsEvent.Table.Schema)
				table := string(rowsEvent.Table.Table)

				// Count rows for this event. Updates have before/after pairs.
				rowCount := len(rowsEvent.Rows)
				// For UPDATE events, rows are pairs of before/after
				if e.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
					rowCount = rowCount / 2
				}

				// Get table info for schema-based operations
				var tblInfo *schema.Table
				if schemaRegistry != nil {
					tblInfo = schemaRegistry.GetTableInfo(schemaName, table)
					if tblInfo != nil && *verbose {
						fmt.Printf("Found schema for %s.%s with %d columns\n",
							schemaName, table, len(tblInfo.Columns))
					}
				}

				// default event time from header
				eventTime := time.Unix(int64(e.Header.Timestamp), 0)

				// If schema available and timeCol configured, try to extract from row values
				if tblInfo != nil && *timeCol != "" {
					// find time column index (schema columns stored lowercase)
					timeIdx := -1
					for i, c := range tblInfo.Columns {
						if strings.EqualFold(c.Name, *timeCol) {
							timeIdx = i
							break
						}
					}
					if timeIdx >= 0 {
						// scan rows for earliest non-zero time
						var earliest time.Time
						for _, rowSlice := range rowsEvent.Rows {
							if timeIdx < len(rowSlice) {
								raw := rowSlice[timeIdx]
								var t time.Time
								switch v := raw.(type) {
								case time.Time:
									t = v
								case []byte:
									s := string(v)
									if parsed, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
										t = parsed
									} else if parsed, err := time.Parse(time.RFC3339, s); err == nil {
										t = parsed
									}
								default:
									// fallback: ignore
								}
								if !t.IsZero() {
									if earliest.IsZero() || t.Before(earliest) {
										earliest = t
									}
								}
							}
						}
						if !earliest.IsZero() {
							eventTime = earliest
						}
					}
				}

				switch e.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					statistics.RecordOperation(schemaName, table, "INSERT", rowCount, eventTime)

					// Decode and display row data if requested
					if *decodeRows {
						decodeAndDisplayRowData(rowsEvent, tblInfo, "INSERT", schemaName, table, e)
					}

					if *jsonOutput {
						// Extract values from first row (for INSERT events)
						var extractedValues map[string]any
						if len(rowsEvent.Rows) > 0 && len(extractColsList) > 0 {
							extractedValues = extractColumnValues(rowsEvent.Rows[0], tblInfo, extractColsList)
						}

						record := EventRecord{
							EventType:       "INSERT",
							Timestamp:       eventTime.Format(time.RFC3339),
							ThreadID:        extractThreadId(lastQuery),
							ServerID:        e.Header.ServerID,
							Schema:          schemaName,
							Table:           table,
							RowsAffected:    rowCount,
							QueryType:       analyzeQueryType(lastQuery),
							ExtractedValues: extractedValues,
							TransactionID:   currentGTID,
							Query:           lastQuery,
						}

						if err := encoder.Encode(record); err != nil {
							fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
						}
					}

					// Detect large inserts
					if *largeThreshold > 0 && rowCount >= *largeThreshold {
						fmt.Printf("\n*** LARGE INSERT DETECTED ***\n")
						fmt.Printf("File: %s\n", *binlogFile)
						fmt.Printf("Event pos: %d - size: %d - time: %d\n", e.Header.LogPos, e.Header.EventSize, e.Header.Timestamp)
						fmt.Printf("Table: %s.%s\n", schemaName, table)
						fmt.Printf("Rows in event: %d\n", rowCount)
						// Dump the event (raw) for context
						e.Dump(os.Stdout)
					}
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					statistics.RecordOperation(schemaName, table, "UPDATE", rowCount, eventTime)

					// Decode and display row data if requested
					if *decodeRows {
						decodeAndDisplayRowData(rowsEvent, tblInfo, "UPDATE", schemaName, table, e)
					}

					if *jsonOutput {
						// For UPDATE, extract from the first "after" row image.
						var extractedValues map[string]any
						if afterRow := getAfterImageRow(rowsEvent.Rows); afterRow != nil && len(extractColsList) > 0 {
							extractedValues = extractColumnValues(afterRow, tblInfo, extractColsList)
						}
						record := EventRecord{
							EventType:       "UPDATE",
							Timestamp:       eventTime.Format(time.RFC3339),
							ThreadID:        extractThreadId(lastQuery),
							ServerID:        e.Header.ServerID,
							Schema:          schemaName,
							Table:           table,
							RowsAffected:    rowCount,
							QueryType:       analyzeQueryType(lastQuery),
							ExtractedValues: extractedValues,
							TransactionID:   currentGTID,
							Query:           lastQuery,
						}

						if err := encoder.Encode(record); err != nil {
							fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
						}
					}

					if *largeThreshold > 0 && rowCount >= *largeThreshold {
						fmt.Printf("\n*** LARGE UPDATE DETECTED ***\n")
						fmt.Printf("File: %s\n", *binlogFile)
						fmt.Printf("Event pos: %d - size: %d - time: %d\n", e.Header.LogPos, e.Header.EventSize, e.Header.Timestamp)
						fmt.Printf("Table: %s.%s\n", schemaName, table)
						fmt.Printf("Rows affected (approx): %d\n", rowCount)
						e.Dump(os.Stdout)
					}
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					statistics.RecordOperation(schemaName, table, "DELETE", rowCount, eventTime)

					// Decode and display row data if requested
					if *decodeRows {
						decodeAndDisplayRowData(rowsEvent, tblInfo, "DELETE", schemaName, table, e)
					}

					if *jsonOutput {
						// For DELETE, extract from the deleted row values
						var extractedValues map[string]any
						if len(rowsEvent.Rows) > 0 && len(extractColsList) > 0 {
							extractedValues = extractColumnValues(rowsEvent.Rows[0], tblInfo, extractColsList)
						}

						record := EventRecord{
							EventType:       "DELETE",
							Timestamp:       eventTime.Format(time.RFC3339),
							ThreadID:        extractThreadId(lastQuery),
							ServerID:        e.Header.ServerID,
							Schema:          schemaName,
							Table:           table,
							RowsAffected:    rowCount,
							QueryType:       analyzeQueryType(lastQuery),
							ExtractedValues: extractedValues,
							TransactionID:   currentGTID,
							Query:           lastQuery,
						}

						if err := encoder.Encode(record); err != nil {
							fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
						}
					}

					if *largeThreshold > 0 && rowCount >= *largeThreshold {
						fmt.Printf("\n*** LARGE DELETE DETECTED ***\n")
						fmt.Printf("File: %s\n", *binlogFile)
						fmt.Printf("Event pos: %d - size: %d - time: %d\n", e.Header.LogPos, e.Header.EventSize, e.Header.Timestamp)
						fmt.Printf("Table: %s.%s\n", schemaName, table)
						fmt.Printf("Rows in event: %d\n", rowCount)
						e.Dump(os.Stdout)
					}
				}
			}
		} else if eventsFound && eventStartPos > uint32(startPosition) && !*parseAll {
			if *stopAtNext {
				// We've found the next position after our target
				return fmt.Errorf("%w at position %d (previous events ended at %d)",
					errFoundNextEvent, eventStartPos, e.Header.LogPos-uint32(e.Header.EventSize))
			}
		}
		return nil
	})

	parseFailed := false
	if err != nil {
		if !errors.Is(err, errFoundNextEvent) {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			parseFailed = true
		} else {
			fmt.Println(err.Error())
		}
	}

	// At the end of processing, if showStats is true:
	if *showStats {
		statistics.PrintStats()
		if schemaRegistry != nil {
			schemaRegistry.PrintWarnings()
		}
	}

	if parseFailed {
		os.Exit(1)
	}
}

func listAllLogPositions(binlogFile string) {
	p := replication.NewBinlogParser()
	err := p.ParseFile(binlogFile, 4, func(e *replication.BinlogEvent) error {
		fmt.Printf("Log position: %d\n", e.Header.LogPos)
		return nil
	})

	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
