package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ChaosHour/go-parse/pkg/schema"
	"github.com/ChaosHour/go-parse/pkg/stats" // Updated import path
	"github.com/go-mysql-org/go-mysql/replication"
)

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

var (
	binlogFile    = flag.String("file", "", "Binlog file to parse")
	offset        = flag.Int64("offset", -1, "Starting offset (use -1 to ignore)")
	logPosition   = flag.Int64("logPosition", -1, "Log position to start from (use -1 to ignore)")
	listPositions = flag.Bool("listPositions", false, "List all log positions in the binlog")
	stopAtNext    = flag.Bool("stopAtNext", false, "Stop at the next log position")
	showStats     = flag.Bool("showStats", false, "Show operation statistics by database and table")
	verbose       = flag.Bool("verbose", false, "Show detailed position information for each event")
	parseAll      = flag.Bool("all", false, "Parse entire binlog file")
	schemaFile    = flag.String("schema", "", "MySQL schema dump file to load")
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
		fmt.Fprintf(os.Stderr, "Usage: %s -file <binlog file> [-all] [-offset <offset>] [-logPosition <log position>] [-listPositions] [-stopAtNext] [-showStats] [-verbose] [-schema <schema file>]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

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

	p := replication.NewBinlogParser()
	var eventsFound bool
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

			// Only dump event details if not showing stats
			if !*showStats {
				// Replace default event dumping with custom formatting
				if tableMap, ok := e.Event.(*replication.TableMapEvent); ok {
					dumpTableMapEvent(tableMap)
				} else {
					e.Dump(os.Stdout)
				}
			}

			// Add statistics tracking for different event types
			if rowsEvent, ok := e.Event.(*replication.RowsEvent); ok {
				schema := string(rowsEvent.Table.Schema)
				table := string(rowsEvent.Table.Table)

				// Update schema validation
				if schemaRegistry != nil {
					tableInfo := schemaRegistry.GetTableInfo(schema, table)
					if tableInfo != nil && *verbose {
						fmt.Printf("Found schema for %s.%s with %d columns\n",
							schema, table, len(tableInfo.Columns))
					}
				}

				switch e.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					statistics.RecordOperation(schema, table, "INSERT", len(rowsEvent.Rows))
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					statistics.RecordOperation(schema, table, "UPDATE", len(rowsEvent.Rows)/2) // Divide by 2 as updates have before/after rows
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					statistics.RecordOperation(schema, table, "DELETE", len(rowsEvent.Rows))
				}
			}
		} else if eventsFound && eventStartPos > uint32(startPosition) && !*parseAll {
			if *stopAtNext {
				// We've found the next position after our target
				return fmt.Errorf("found next event at position %d (previous events ended at %d)",
					eventStartPos, e.Header.LogPos-uint32(e.Header.EventSize))
			}
		}
		return nil
	})

	if err != nil {
		if !strings.HasPrefix(err.Error(), "found next event") {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println(err.Error())
		}
	}

	// At the end of processing, if showStats is true:
	if *showStats {
		statistics.Print()
		if schemaRegistry != nil {
			schemaRegistry.PrintWarnings()
		}
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
