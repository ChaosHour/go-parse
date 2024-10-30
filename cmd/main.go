package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ChaosHour/go-parse/pkg/schema"
	_ "github.com/ChaosHour/go-parse/pkg/schema"
	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	binlogFile    = flag.String("file", "", "Binlog file to parse")
	offset        = flag.Int64("offset", -1, "Starting offset (use -1 to ignore)")
	logPosition   = flag.Int64("logPosition", -1, "Log position to start from (use -1 to ignore)")
	listPositions = flag.Bool("listPositions", false, "List all log positions in the binlog")
	stopAtNext    = flag.Bool("stopAtNext", false, "Stop at the next log position")
	showInserts   = flag.Bool("showInserts", false, "Show INSERT operations")
	showUpdates   = flag.Bool("showUpdates", false, "Show UPDATE operations")
	showDeletes   = flag.Bool("showDeletes", false, "Show DELETE operations")
	tableName     = flag.String("table", "", "Filter by table name")
	jsonOutput    = flag.Bool("json", false, "Output in JSON format")
	schemaName    = flag.String("schema", "", "Filter by schema/database name")
	showSummary   = flag.Bool("summary", false, "Show summary of operations")
	gtidFilter    = flag.String("gtid", "", "Filter by GTID (format: uuid:transaction_id)")
	showGTIDs     = flag.Bool("showGTIDs", false, "Show GTID events")
	schemaFile    = flag.String("schema-file", "", "JSON file containing table schemas")
)

type DMLEvent struct {
	Operation string                 `json:"operation"`
	Table     string                 `json:"table"`
	Database  string                 `json:"database"`
	Timestamp uint32                 `json:"timestamp"`
	LogPos    uint32                 `json:"logPosition"`
	RowValues map[string]interface{} `json:"rowValues,omitempty"`
	OldValues map[string]interface{} `json:"oldValues,omitempty"`
	GTID      string                 `json:"gtid,omitempty"`
}

type DMLStats struct {
	Inserts    int                   `json:"inserts"`
	Updates    int                   `json:"updates"`
	Deletes    int                   `json:"deletes"`
	TableStats map[string]TableStats `json:"tableStats,omitempty"`
}

type TableStats struct {
	Inserts int `json:"inserts"`
	Updates int `json:"updates"`
	Deletes int `json:"deletes"`
}

var stats = DMLStats{
	TableStats: make(map[string]TableStats),
}

// Add this variable to track current GTID
var currentGTID string

// Add new function to parse GTID
func parseGTIDFilter(gtid string) (string, uint64, error) {
	if gtid == "" {
		return "", 0, nil
	}

	parts := strings.Split(gtid, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid GTID format. Expected uuid:transaction_id")
	}

	return parts[0], 0, nil // We'll just match the UUID part for simplicity
}

// Add this helper function somewhere in the file
func formatSID(sid []byte) string {
	if len(sid) != 16 {
		return ""
	}
	uuid := hex.EncodeToString(sid)
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		uuid[0:8], uuid[8:12], uuid[12:16], uuid[16:20], uuid[20:32])
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -file <binlog file> [-offset <offset>] [-logPosition <log position>] [-listPositions] [-stopAtNext] [-showInserts] [-showUpdates] [-showDeletes] [-table <table name>] [-json] [-schema <schema name>] [-summary] [-gtid <gtid>] [-showGTIDs] [-schema-file <schema file>]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	fmt.Printf("binlogFile: %s\n", *binlogFile)
	fmt.Printf("logPosition: %d\n", *logPosition)
	fmt.Printf("stopAtNext: %v\n", *stopAtNext)

	if *binlogFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	if _, err := os.Stat(*binlogFile); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: Binlog file %s does not exist\n", *binlogFile)
		os.Exit(1)
	}

	if *listPositions {
		listAllLogPositions(*binlogFile)
		return
	}

	startPosition := *offset
	if startPosition == -1 && *logPosition != -1 {
		startPosition = *logPosition
	}

	if startPosition == -1 {
		fmt.Fprintf(os.Stderr, "Error: Either offset or log position must be specified\n")
		flag.Usage()
		os.Exit(1)
	}

	// Parse GTID filter if provided
	_, _, err := parseGTIDFilter(*gtidFilter) // Remove gtidUUID variable
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *schemaFile != "" {
		if err := schema.LoadSchemaFile(*schemaFile); err != nil {
			fmt.Fprintf(os.Stderr, "Error loading schema file: %v\n", err)
			os.Exit(1)
		}
	}

	var foundStart bool
	var processedEvent bool

	fmt.Println("Starting binlog parsing...")
	p := replication.NewBinlogParser()
	err = p.ParseFile(*binlogFile, startPosition, func(e *replication.BinlogEvent) error {
		if e.Header.LogPos >= uint32(startPosition) {
			e.Dump(os.Stdout)
			if *stopAtNext && e.Header.LogPos > uint32(startPosition) {
				fmt.Printf("reached log position %d\n", startPosition)
				return fmt.Errorf("STOP")
			}
		}
		return nil
	})

	if err != nil && err.Error() != "STOP" {
		fmt.Println("Error:", err)
	}

	if *showSummary {
		printSummary()
	}
}

func handleRowsEvent(header *replication.EventHeader, event *replication.RowsEvent) error {
	// Schema filter check
	if *schemaName != "" && !strings.EqualFold(*schemaName, string(event.Table.Schema)) {
		return nil
	}

	// Table filter check
	tableKey := fmt.Sprintf("%s.%s", string(event.Table.Schema), string(event.Table.Table))
	if *tableName != "" && !strings.EqualFold(*tableName, string(event.Table.Table)) {
		return nil
	}

	// Initialize table stats if not exists
	if _, exists := stats.TableStats[tableKey]; !exists {
		stats.TableStats[tableKey] = TableStats{}
	}

	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		stats.Inserts++
		tableStats := stats.TableStats[tableKey]
		tableStats.Inserts++
		stats.TableStats[tableKey] = tableStats
		if *showInserts || (!*showUpdates && !*showDeletes) {
			handleInsert(header, event)
		}
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		stats.Updates++
		tableStats := stats.TableStats[tableKey]
		tableStats.Updates++
		stats.TableStats[tableKey] = tableStats
		if *showUpdates || (!*showInserts && !*showDeletes) {
			handleUpdate(header, event)
		}
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		stats.Deletes++
		tableStats := stats.TableStats[tableKey]
		tableStats.Deletes++
		stats.TableStats[tableKey] = tableStats
		if *showDeletes || (!*showInserts && !*showUpdates) {
			handleDelete(header, event)
		}
	}
	return nil
}

func handleInsert(header *replication.EventHeader, event *replication.RowsEvent) {
	for _, row := range event.Rows {
		dmlEvent := createDMLEvent("INSERT", header, event, row, nil)
		outputEvent(dmlEvent)
	}
}

func handleUpdate(header *replication.EventHeader, event *replication.RowsEvent) {
	for i := 0; i < len(event.Rows); i += 2 {
		dmlEvent := createDMLEvent("UPDATE", header, event, event.Rows[i+1], event.Rows[i])
		outputEvent(dmlEvent)
	}
}

func handleDelete(header *replication.EventHeader, event *replication.RowsEvent) {
	for _, row := range event.Rows {
		dmlEvent := createDMLEvent("DELETE", header, event, row, nil)
		outputEvent(dmlEvent)
	}
}

func createDMLEvent(operation string, header *replication.EventHeader, event *replication.RowsEvent, row []interface{}, oldRow []interface{}) DMLEvent {
	dmlEvent := DMLEvent{
		Operation: operation,
		Table:     string(event.Table.Table),
		Database:  string(event.Table.Schema),
		Timestamp: header.Timestamp,
		LogPos:    header.LogPos,
		RowValues: make(map[string]interface{}),
		GTID:      currentGTID,
	}

	// Get table definition if available
	tableDef := schema.GetTableDef(string(event.Table.Schema), string(event.Table.Table))

	if tableDef != nil {
		// Map values to column names using schema
		for i, val := range row {
			if i < len(tableDef.Columns) {
				dmlEvent.RowValues[tableDef.Columns[i].Name] = val
			}
		}

		if oldRow != nil {
			dmlEvent.OldValues = make(map[string]interface{})
			for i, val := range oldRow {
				if i < len(tableDef.Columns) {
					dmlEvent.OldValues[tableDef.Columns[i].Name] = val
				}
			}
		}
	} else {
		// Fallback to numeric indices if no schema is available
		for i, val := range row {
			dmlEvent.RowValues[fmt.Sprintf("col_%d", i)] = val
		}
		if oldRow != nil {
			dmlEvent.OldValues = make(map[string]interface{})
			for i, val := range oldRow { // Fixed: Added proper loop variables
				dmlEvent.OldValues[fmt.Sprintf("col_%d", i)] = val
			}
		}
	}

	return dmlEvent
}

func outputEvent(event DMLEvent) {
	if *jsonOutput {
		if data, err := json.MarshalIndent(event, "", "  "); err == nil {
			fmt.Println(string(data))
		}
	} else {
		fmt.Printf("=== %s === Pos: %d, Time: %d\n", event.Operation, event.LogPos, event.Timestamp)
		fmt.Printf("Database: %s, Table: %s\n", event.Database, event.Table)
		if event.GTID != "" {
			fmt.Printf("GTID: %s\n", event.GTID)
		}
		if event.OldValues != nil {
			fmt.Println("Old Values:", event.OldValues)
		}
		fmt.Println("Values:", event.RowValues)
		fmt.Println()
	}
}

func listAllLogPositions(binlogFile string) {
	p := replication.NewBinlogParser()
	err := p.ParseFile(binlogFile, 4, func(e *replication.BinlogEvent) error {
		fmt.Printf("Log position: %d\n", e.Header.LogPos)
		return nil
	})

	if err != nil {
		fmt.Println(err.Error())
	}
}

func printSummary() {
	if !*showSummary {
		return
	}

	fmt.Println("\n=== Operation Summary ===")
	if *jsonOutput {
		if data, err := json.MarshalIndent(stats, "", "  "); err == nil {
			fmt.Println(string(data))
			return
		}
	}

	fmt.Printf("Total Operations:\n")
	fmt.Printf("  Inserts: %d\n", stats.Inserts)
	fmt.Printf("  Updates: %d\n", stats.Updates)
	fmt.Printf("  Deletes: %d\n", stats.Deletes)
	fmt.Printf("\nPer Table Statistics:\n")

	for table, tableStats := range stats.TableStats {
		fmt.Printf("\n%s:\n", table)
		fmt.Printf("  Inserts: %d\n", tableStats.Inserts)
		fmt.Printf("  Updates: %d\n", tableStats.Updates)
		fmt.Printf("  Deletes: %d\n", tableStats.Deletes)
	}
}
