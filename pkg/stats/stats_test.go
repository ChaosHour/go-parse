package stats

import (
	"testing"
	"time"
)

func TestRecordOperationUpdatesStats(t *testing.T) {
	s := NewStatistics()
	base := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)

	s.RecordOperation("testdb", "t1", "INSERT", 10, base)
	s.RecordOperation("testdb", "t1", "INSERT", 5, base.Add(1*time.Minute))
	s.RecordOperation("testdb", "t1", "UPDATE", 2, base.Add(2*time.Minute))

	tblStats, ok := s.Stats["testdb"]["t1"]
	if !ok {
		t.Fatal("expected stats for testdb.t1 to exist")
	}

	insertStats, ok := tblStats.Operations["INSERT"]
	if !ok {
		t.Fatal("expected INSERT stats to exist")
	}
	if insertStats.Count != 2 {
		t.Fatalf("expected INSERT count 2, got %d", insertStats.Count)
	}
	if insertStats.RowCount != 15 {
		t.Fatalf("expected INSERT row count 15, got %d", insertStats.RowCount)
	}
	if !insertStats.FirstSeen.Equal(base) {
		t.Fatalf("expected INSERT first seen %v, got %v", base, insertStats.FirstSeen)
	}
	if !insertStats.LastSeen.Equal(base.Add(1*time.Minute)) {
		t.Fatalf("expected INSERT last seen %v, got %v", base.Add(1*time.Minute), insertStats.LastSeen)
	}

	updateStats, ok := tblStats.Operations["UPDATE"]
	if !ok {
		t.Fatal("expected UPDATE stats to exist")
	}
	if updateStats.Count != 1 {
		t.Fatalf("expected UPDATE count 1, got %d", updateStats.Count)
	}
	if updateStats.RowCount != 2 {
		t.Fatalf("expected UPDATE row count 2, got %d", updateStats.RowCount)
	}
}
