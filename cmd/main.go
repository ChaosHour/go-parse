package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ChaosHour/go-parse/pkg/stats" // Updated import path
	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	binlogFile    = flag.String("file", "", "Binlog file to parse")
	offset        = flag.Int64("offset", -1, "Starting offset (use -1 to ignore)")
	logPosition   = flag.Int64("logPosition", -1, "Log position to start from (use -1 to ignore)")
	listPositions = flag.Bool("listPositions", false, "List all log positions in the binlog")
	stopAtNext    = flag.Bool("stopAtNext", false, "Stop at the next log position")
	showStats     = flag.Bool("showStats", false, "Show operation statistics by database and table")
	verbose       = flag.Bool("verbose", false, "Show detailed position information for each event")
	parseAll      = flag.Bool("all", false, "Parse entire binlog file")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -file <binlog file> [-all] [-offset <offset>] [-logPosition <log position>] [-listPositions] [-stopAtNext] [-showStats] [-verbose]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

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

	// Create statistics collector
	stats := stats.NewStatistics()

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
				e.Dump(os.Stdout)
			}

			// Add statistics tracking for different event types
			if rowsEvent, ok := e.Event.(*replication.RowsEvent); ok {
				schema := string(rowsEvent.Table.Schema)
				table := string(rowsEvent.Table.Table)
				switch e.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					stats.RecordOperation(schema, table, "INSERT", len(rowsEvent.Rows))
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					stats.RecordOperation(schema, table, "UPDATE", len(rowsEvent.Rows)/2) // Divide by 2 as updates have before/after rows
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					stats.RecordOperation(schema, table, "DELETE", len(rowsEvent.Rows))
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
		stats.Print()
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
