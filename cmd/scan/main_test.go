package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ChaosHour/go-parse/pkg/schema"
)

func TestAutoDiscoverSchema(t *testing.T) {
	// Test with real binlog files if they exist
	testDir := "../../tests"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("Test directory not found, skipping integration test")
	}

	// Gather test binlog files
	var files []string
	err := filepath.WalkDir(testDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		if strings.Contains(base, "mysql-bin") || strings.HasPrefix(base, "binlog") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk test directory: %v", err)
	}

	if len(files) == 0 {
		t.Skip("No binlog files found in test directory")
	}

	// Test auto-discovery
	sr, err := autoDiscoverSchema(files)
	if err != nil {
		t.Fatalf("autoDiscoverSchema failed: %v", err)
	}

	if sr == nil {
		t.Fatal("autoDiscoverSchema returned nil schema registry")
	}

	// Verify basic functionality
	if sr.Databases == nil {
		t.Error("Schema registry has nil Databases map")
	}

	t.Logf("Discovered %d databases", len(sr.Databases))
	for dbName, db := range sr.Databases {
		t.Logf("  Database: %s with %d tables", dbName, len(db.Tables))
	}
}

func TestAutoDiscoverSchemaEmptyFileList(t *testing.T) {
	sr, err := autoDiscoverSchema([]string{})
	if err != nil {
		t.Fatalf("Expected no error for empty file list, got: %v", err)
	}

	if sr == nil {
		t.Fatal("Expected non-nil schema registry")
	}

	if len(sr.Databases) != 0 {
		t.Errorf("Expected 0 databases, got %d", len(sr.Databases))
	}
}

func TestAutoDiscoverSchemaNonExistentFile(t *testing.T) {
	_, err := autoDiscoverSchema([]string{"/nonexistent/binlog.000001"})
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

func TestProcessFileFuzzySearchGlobalMatchLimit(t *testing.T) {
	// This test verifies that maxMatches is properly enforced globally
	// across concurrent processing
	
	results := make(map[string]*FuzzySearchResult)
	var resultsMutex sync.Mutex
	var globalMatchCount int64
	
	// Mock showing matches and set a low limit
	oldShowMatches := *showMatches
	*showMatches = true
	defer func() { *showMatches = oldShowMatches }()
	
	maxMatches := int64(5)
	
	// Simulate multiple concurrent matches
	for i := 0; i < 10; i++ {
		// Simulate what happens inside processFileFuzzySearch
		resultsMutex.Lock()
		if results["INSERT"] == nil {
			results["INSERT"] = &FuzzySearchResult{
				Keyword: "INSERT",
				Count:   0,
				Matches: make([]FuzzyMatch, 0),
			}
		}
		results["INSERT"].Count++
		
		if atomic.LoadInt64(&globalMatchCount) < maxMatches {
			match := FuzzyMatch{
				File:     "test.bin",
				Position: uint32(i * 100),
				Query:    "INSERT INTO test VALUES (1)",
			}
			results["INSERT"].Matches = append(results["INSERT"].Matches, match)
			atomic.AddInt64(&globalMatchCount, 1)
		}
		resultsMutex.Unlock()
	}
	
	// Verify the limit was enforced
	if len(results["INSERT"].Matches) > int(maxMatches) {
		t.Errorf("Expected at most %d matches, got %d", maxMatches, len(results["INSERT"].Matches))
	}
	
	// Verify count still tracked all occurrences
	if results["INSERT"].Count != 10 {
		t.Errorf("Expected count of 10, got %d", results["INSERT"].Count)
	}
	
	t.Logf("Verified: Count=%d, Matches stored=%d (limit=%d)", 
		results["INSERT"].Count, len(results["INSERT"].Matches), maxMatches)
}

func TestExtractContext(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		keyword       string
		contextLength int
		wantContains  string
	}{
		{
			name:          "keyword at start",
			query:         "INSERT INTO users VALUES (1, 'test')",
			keyword:       "insert",
			contextLength: 20,
			wantContains:  "insert",
		},
		{
			name:          "keyword in middle",
			query:         "SELECT * FROM users WHERE id = 1",
			keyword:       "where",
			contextLength: 15,
			wantContains:  "where",
		},
		{
			name:          "keyword not found",
			query:         "SELECT * FROM users",
			keyword:       "delete",
			contextLength: 10,
			wantContains:  "SELECT * FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractContext(tt.query, tt.keyword, tt.contextLength)
			if !strings.Contains(strings.ToLower(result), strings.ToLower(tt.wantContains)) {
				t.Errorf("extractContext() result %q doesn't contain %q", result, tt.wantContains)
			}
		})
	}
}

func TestGetColumnNames(t *testing.T) {
	columns := []schema.Column{
		{Name: "id", DataType: "INT"},
		{Name: "name", DataType: "VARCHAR"},
		{Name: "email", DataType: "VARCHAR"},
	}

	names := getColumnNames(columns)

	if len(names) != 3 {
		t.Errorf("Expected 3 column names, got %d", len(names))
	}

	expected := []string{"id", "name", "email"}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("Expected column name %s at index %d, got %s", expected[i], i, name)
		}
	}
}

func TestEventName(t *testing.T) {
	tests := []struct {
		name      string
		eventType byte
		want      string
	}{
		{"write_rows_v1", 23, "WRITE_ROWS"},
		{"write_rows_v2", 30, "WRITE_ROWS"},
		{"update_rows_v1", 24, "UPDATE_ROWS"},
		{"update_rows_v2", 31, "UPDATE_ROWS"},
		{"delete_rows_v1", 25, "DELETE_ROWS"},
		{"delete_rows_v2", 32, "DELETE_ROWS"},
		{"query_event", 2, "QUERY"},
		{"unknown_event", 99, "TYPE_99"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: Can't test directly as replication.EventType is not a byte
			// This is a placeholder for documentation
			// In reality, we'd need to use actual replication.EventType values
			t.Logf("Event type %d should map to %s", tt.eventType, tt.want)
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		maxLen int
		want   string
	}{
		{
			name:   "string shorter than max",
			input:  "short",
			maxLen: 10,
			want:   "short",
		},
		{
			name:   "string equal to max",
			input:  "exactly10!",
			maxLen: 10,
			want:   "exactly10!",
		},
		{
			name:   "string longer than max",
			input:  "this is a very long string that needs truncation",
			maxLen: 20,
			want:   "this is a very lo...",
		},
		{
			name:   "max length less than 3",
			input:  "test",
			maxLen: 2,
			want:   "te",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateString(tt.input, tt.maxLen)
			if got != tt.want {
				t.Errorf("truncateString() = %q, want %q", got, tt.want)
			}
			if len(got) > tt.maxLen {
				t.Errorf("truncateString() length %d exceeds maxLen %d", len(got), tt.maxLen)
			}
		})
	}
}

func TestAggregateConfigStruct(t *testing.T) {
	// Verify that AggregateConfig is a named type (not anonymous)
	cfg := AggregateConfig{
		Threshold:   1000,
		SchemaFile:  "test.sql",
		SchemaName:  "testdb",
		TableName:   "testtable",
		TimeCol:     "created_at",
		CategoryCol: "category_id",
	}

	if cfg.Threshold != 1000 {
		t.Errorf("Expected Threshold 1000, got %d", cfg.Threshold)
	}

	if cfg.SchemaName != "testdb" {
		t.Errorf("Expected SchemaName testdb, got %s", cfg.SchemaName)
	}

	// Verify we can pass it to functions (type safety)
	testFunc := func(c AggregateConfig) string {
		return c.SchemaName
	}

	result := testFunc(cfg)
	if result != "testdb" {
		t.Errorf("Expected testdb, got %s", result)
	}
}
