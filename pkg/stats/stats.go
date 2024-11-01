package stats

import (
	"fmt"
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
	fmt.Printf("\nParsing Statistics:\n")
	fmt.Printf("Total Events: %d\n", s.TotalEvents)
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

		for db, tables := range s.Stats {
			fmt.Printf("\nDatabase: %s\n", db)
			fmt.Println("--------------------")

			for table, stats := range tables {
				fmt.Printf("\nTable: %s\n", table)
				for op, opStats := range stats.Operations {
					fmt.Printf("  %-7s: %d operations affecting %d rows\n",
						op, opStats.Count, opStats.RowCount)
					totalOps += opStats.Count
					totalRows += opStats.RowCount
				}
			}
		}

		fmt.Println("\nSummary:")
		fmt.Println("--------")
		fmt.Printf("Total operations: %d\n", totalOps)
		fmt.Printf("Total rows affected: %d\n", totalRows)
	}
}

// Alias for PrintStats to maintain backward compatibility
func (s *Statistics) Print() {
	s.PrintStats()
}
