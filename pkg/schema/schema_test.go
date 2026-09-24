package schema

import (
	"os"
	"reflect"
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

func TestLoadFromFileKeepsColumnOrderWithCommasInTypes(t *testing.T) {
	tempFile, err := os.CreateTemp("", "schema-commas-*.sql")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	content := "-- Database: shop\n" +
		"CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL AUTO_INCREMENT,\n" +
		"  `amount` decimal(10,2) NOT NULL DEFAULT '0.00',\n" +
		"  `status` enum('new','paid','shipped') NOT NULL DEFAULT 'new',\n" +
		"  `note` varchar(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `k_status` (`status`,`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

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

	table := sr.GetTableInfo("shop", "orders")
	if table == nil {
		t.Fatal("expected table orders to be parsed")
	}

	// Column positions must line up with the row images in the binlog, so a
	// type containing a comma must not introduce a phantom column.
	wantNames := []string{"id", "amount", "status", "note"}
	gotNames := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		gotNames = append(gotNames, col.Name)
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("columns = %v, want %v", gotNames, wantNames)
	}

	if got, want := table.Columns[1].DataType, "decimal(10,2) NOT NULL DEFAULT '0.00'"; got != want {
		t.Fatalf("amount data type = %q, want %q", got, want)
	}
	if got, want := table.Columns[2].DataType, "enum('new','paid','shipped') NOT NULL DEFAULT 'new'"; got != want {
		t.Fatalf("status data type = %q, want %q", got, want)
	}
}

func TestLoadFromDDLAttachesTablesToDatabaseFromUse(t *testing.T) {
	sr := NewSchemaRegistry()
	err := sr.LoadFromDDL([]string{
		"USE `shop`",
		"CREATE TABLE `orders` (`id` int NOT NULL, `amount` decimal(10,2) NOT NULL, `status` enum('new','paid') NOT NULL, PRIMARY KEY (`id`))",
	})
	if err != nil {
		t.Fatalf("LoadFromDDL() error = %v", err)
	}

	table := sr.GetTableInfo("shop", "orders")
	if table == nil {
		t.Fatal("expected orders to be attached to shop via USE statement")
	}

	wantNames := []string{"id", "amount", "status"}
	gotNames := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		gotNames = append(gotNames, col.Name)
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("columns = %v, want %v", gotNames, wantNames)
	}
}

func TestLoadFromDDLWithoutUseFallsBackToDiscovered(t *testing.T) {
	sr := NewSchemaRegistry()
	if err := sr.LoadFromDDL([]string{
		"CREATE TABLE `t1` (`id` int NOT NULL, `name` varchar(20))",
	}); err != nil {
		t.Fatalf("LoadFromDDL() error = %v", err)
	}

	if table := sr.GetTableInfo("discovered", "t1"); table == nil {
		t.Fatal("expected t1 under the discovered database when no USE was seen")
	}
}
