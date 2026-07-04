package schema

import (
	"os"
	"testing"
)

func TestLoadFromFileParsesTableColumns(t *testing.T) {
	tempFile, err := os.CreateTemp("", "schema-*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	content := `CREATE TABLE testdb.sbtest1 (
  ` + "`id` int(10) unsigned NOT NULL AUTO_INCREMENT," + `
  ` + "`k` int(10) unsigned NOT NULL DEFAULT '0'," + `
  ` + "`c` char(120) COLLATE utf8mb4_bin NOT NULL DEFAULT ''," + `
  ` + "`pad` char(60) COLLATE utf8mb4_bin NOT NULL DEFAULT ''," + `
  PRIMARY KEY (` + "`id`" + `),
  KEY ` + "`k_1` (`k`)" + `
);
`

	if _, err := tempFile.WriteString(content); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("failed to close schema file: %v", err)
	}

	sr := NewSchemaRegistry()
	if err := sr.LoadFromFile(tempFile.Name()); err != nil {
		t.Fatalf("LoadFromFile() error = %v", err)
	}

	table := sr.GetTableInfo("testdb", "sbtest1")
	if table == nil {
		t.Fatal("expected table sbtest1 to be parsed")
	}

	if len(table.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(table.Columns))
	}

	expectedNames := []string{"id", "k", "c", "pad"}
	for i, col := range table.Columns {
		if col.Name != expectedNames[i] {
			t.Fatalf("unexpected column at index %d: got %q, want %q", i, col.Name, expectedNames[i])
		}
	}
}

func TestLoadFromFileUseStatementSetsDefaultDatabase(t *testing.T) {
	tempFile, err := os.CreateTemp("", "schema-use-*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	content := "USE `db1`;\n" +
		"CREATE TABLE t1 (\n" +
		"  `id` int NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n" +
		"use db2;\n" +
		"CREATE TABLE t2 (\n" +
		"  `name` varchar(20) NOT NULL\n" +
		");\n"

	if _, err := tempFile.WriteString(content); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("failed to close schema file: %v", err)
	}

	sr := NewSchemaRegistry()
	if err := sr.LoadFromFile(tempFile.Name()); err != nil {
		t.Fatalf("LoadFromFile() error = %v", err)
	}

	if table := sr.GetTableInfo("db1", "t1"); table == nil {
		t.Fatal("expected t1 to be attached to db1 via USE statement")
	}
	if table := sr.GetTableInfo("db2", "t2"); table == nil {
		t.Fatal("expected t2 to be attached to db2 after second USE statement")
	}
}

func TestGetTableInfoRecordsMissingTableWarning(t *testing.T) {
	sr := NewSchemaRegistry()
	sr.Databases["testdb"] = &Database{Name: "testdb", Tables: map[string]*Table{}}

	table := sr.GetTableInfo("testdb", "missing")
	if table != nil {
		t.Fatal("expected missing table lookup to return nil")
	}

	if len(sr.warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d", len(sr.warnings))
	}
}
