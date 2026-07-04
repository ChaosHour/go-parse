package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/ChaosHour/go-parse/pkg/schema"
)

func TestGetAfterImageRow(t *testing.T) {
	cases := []struct {
		name string
		rows [][]any
		want []any
	}{
		{
			name: "empty rows",
			rows: nil,
			want: nil,
		},
		{
			name: "single row",
			rows: [][]any{{1, "hello"}},
			want: []any{1, "hello"},
		},
		{
			name: "before after pair",
			rows: [][]any{{1, "before"}, {1, "after"}},
			want: []any{1, "after"},
		},
		{
			name: "multiple before/after pairs",
			rows: [][]any{{1, "before1"}, {1, "after1"}, {2, "before2"}, {2, "after2"}},
			want: []any{1, "after1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := getAfterImageRow(tc.rows)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("getAfterImageRow() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExtractColumnValues(t *testing.T) {
	tblInfo := &schema.Table{
		Columns: []schema.Column{
			{Name: "id"},
			{Name: "Name"},
			{Name: "created_at"},
		},
	}
	row := []any{
		int64(42),
		[]byte("alice"),
		time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC),
	}

	got := extractColumnValues(row, tblInfo, []string{"id", "name", "created_at", "missing"})
	want := map[string]any{
		"id":         int64(42),
		"name":       "alice",
		"created_at": "2026-05-03 14:00:00",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extractColumnValues() = %#v, want %#v", got, want)
	}
}
