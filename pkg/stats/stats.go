package stats

import (
	"fmt"
	"time"
)

type DbStats struct {
	Database string
	Table    string
	Inserts  int64
	Updates  int64
	Deletes  int64
}

type Statistics struct {
	EventCounts map[byte]int
	TotalEvents int
	StartTime   time.Time
	Stats       map[string]map[string]*DbStats // db -> table -> stats
}

func NewStatistics() *Statistics {
	return &Statistics{
		EventCounts: make(map[byte]int),
		StartTime:   time.Now(),
		Stats:       make(map[string]map[string]*DbStats),
	}
}

// Record handles both event type recording and DB operation recording
func (s *Statistics) Record(eventType interface{}, table ...string) {
	switch t := eventType.(type) {
	case byte:
		s.EventCounts[t]++
		s.TotalEvents++
	case string:
		if len(table) >= 2 {
			database, tableName := table[0], table[1]
			if _, exists := s.Stats[database]; !exists {
				s.Stats[database] = make(map[string]*DbStats)
			}
			if _, exists := s.Stats[database][tableName]; !exists {
				s.Stats[database][tableName] = &DbStats{
					Database: database,
					Table:    tableName,
				}
			}
			switch eventType {
			case "INSERT":
				s.Stats[database][tableName].Inserts++
			case "UPDATE":
				s.Stats[database][tableName].Updates++
			case "DELETE":
				s.Stats[database][tableName].Deletes++
			}
		}
	}
}

func (s *Statistics) PrintStats() {
	// Print event type statistics
	duration := time.Since(s.StartTime)
	fmt.Printf("\nParsing Statistics:\n")
	fmt.Printf("Total Events: %d\n", s.TotalEvents)
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("\nEvent Type Breakdown:\n")
	for eventType, count := range s.EventCounts {
		fmt.Printf("- Type %d: %d\n", eventType, count)
	}

	// Print DB operation statistics
	if len(s.Stats) > 0 {
		fmt.Printf("\nOperation Statistics by Database/Table:\n")
		fmt.Printf("====================================\n")
		for db, tables := range s.Stats {
			fmt.Printf("\nDatabase: %s\n", db)
			for _, stats := range tables {
				fmt.Printf("  Table: %s\n", stats.Table)
				fmt.Printf("    Inserts: %d\n", stats.Inserts)
				fmt.Printf("    Updates: %d\n", stats.Updates)
				fmt.Printf("    Deletes: %d\n", stats.Deletes)
			}
		}
	}
}

// Alias for PrintStats to maintain backward compatibility
func (s *Statistics) Print() {
	s.PrintStats()
}
