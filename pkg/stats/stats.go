package stats

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Operation struct {
	Count    int
	RowCount int
}

type TableStats struct {
	Operations map[string]*Operation // operation -> stats
}

type Statistics struct {
	EventCounts map[byte]int
	TotalEvents int
	StartTime   time.Time
	Stats       map[string]map[string]*TableStats // db -> table -> stats
}

func NewStatistics() *Statistics {
	return &Statistics{
		EventCounts: make(map[byte]int),
		StartTime:   time.Now(),
		Stats:       make(map[string]map[string]*TableStats),
	}
}

func (s *Statistics) RecordEventType(eventType byte) {
	s.EventCounts[eventType]++
	s.TotalEvents++
}

func (s *Statistics) RecordOperation(database, table, operation string, rowCount int) {
	// Initialize database map if it doesn't exist
	if _, exists := s.Stats[database]; !exists {
		s.Stats[database] = make(map[string]*TableStats)
	}

	// Initialize table stats if they don't exist
	if _, exists := s.Stats[database][table]; !exists {
		s.Stats[database][table] = &TableStats{
			Operations: make(map[string]*Operation),
		}
	}

	// Initialize operation stats if they don't exist
	if _, exists := s.Stats[database][table].Operations[operation]; !exists {
		s.Stats[database][table].Operations[operation] = &Operation{0, 0}
	}

	// Update stats
	s.Stats[database][table].Operations[operation].Count++
	s.Stats[database][table].Operations[operation].RowCount += rowCount
}

func (s *Statistics) PrintStats() {
	duration := time.Since(s.StartTime)
	opsPerSec := float64(s.TotalEvents) / duration.Seconds()

	fmt.Printf("\nParsing Statistics:\n")
	fmt.Printf("Total Events: %d (%.2f ops/sec)\n", s.TotalEvents, opsPerSec)
	fmt.Printf("Duration: %v\n", duration)

	if len(s.EventCounts) > 0 {
		fmt.Printf("\nEvent Type Breakdown:\n")
		for eventType, count := range s.EventCounts {
			fmt.Printf("- Type %d: %d\n", eventType, count)
		}
	}

	if len(s.Stats) > 0 {
		fmt.Printf("\nOperation Statistics:\n")
		fmt.Printf("====================\n")

		var totalOps, totalRows int

		// Sort databases for consistent output
		dbNames := make([]string, 0, len(s.Stats))
		for db := range s.Stats {
			dbNames = append(dbNames, db)
		}
		sort.Strings(dbNames)

		for _, db := range dbNames {
			tables := s.Stats[db]
			fmt.Printf("\nDatabase: %s\n", db)
			fmt.Printf("%s\n", strings.Repeat("-", len(db)+10))

			// Sort tables for consistent output
			tableNames := make([]string, 0, len(tables))
			for table := range tables {
				tableNames = append(tableNames, table)
			}
			sort.Strings(tableNames)

			for _, table := range tableNames {
				stats := tables[table]
				fmt.Printf("\nTable: %s\n", table)

				// Sort operations for consistent output
				ops := make([]string, 0, len(stats.Operations))
				for op := range stats.Operations {
					ops = append(ops, op)
				}
				sort.Strings(ops)

				for _, op := range ops {
					opStats := stats.Operations[op]
					fmt.Printf("  %-7s: %d operations affecting %d rows (avg %.1f rows/op)\n",
						op, opStats.Count, opStats.RowCount,
						float64(opStats.RowCount)/float64(opStats.Count))
					totalOps += opStats.Count
					totalRows += opStats.RowCount
				}
			}
		}

		fmt.Printf("\nSummary:\n")
		fmt.Printf("--------\n")
		fmt.Printf("Total operations: %d\n", totalOps)
		fmt.Printf("Total rows affected: %d\n", totalRows)
		fmt.Printf("Average rows per operation: %.1f\n", float64(totalRows)/float64(totalOps))
		fmt.Printf("Operations per second: %.2f\n", float64(totalOps)/duration.Seconds())
	}
}

// Alias for PrintStats to maintain backward compatibility
func (s *Statistics) Print() {
	s.PrintStats()
}
