package schema

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type ColumnDef struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type TableDef struct {
	Name    string      `json:"name"`
	Schema  string      `json:"schema"`
	Columns []ColumnDef `json:"columns"`
}

type SchemaDef struct {
	Tables []TableDef `json:"tables"`
}

var SchemaDefinitions = make(map[string]*TableDef)

var (
	CreateTableRegex = regexp.MustCompile(`(?i)CREATE TABLE\s+` +
		`(?:[^.]+\.)?([^\s(]+)\s*\((.*?)\)[^;]*;`)
	ColumnDefRegex = regexp.MustCompile(`\s*` +
		`([^\s,]+)\s+` + // Column name
		`([^,\s]+(?:\s+[^,\s]+)*)\s*` + // Column type
		`([^,]*),?`) // Optional modifiers
)

func LoadSchemaFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %v", err)
	}

	// Try JSON first
	if err := parseJSONSchema(data); err == nil {
		return nil
	}

	// If JSON fails, try SQL
	return parseSQLSchema(filename)
}

func parseJSONSchema(data []byte) error {
	var schema SchemaDef
	if err := json.Unmarshal(data, &schema); err != nil {
		return err
	}

	for i, table := range schema.Tables {
		key := fmt.Sprintf("%s.%s", table.Schema, table.Name)
		SchemaDefinitions[key] = &schema.Tables[i]
	}

	return nil
}

func parseSQLSchema(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	var currentSchema string
	scanner := bufio.NewScanner(file)
	var buffer strings.Builder

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "/*") {
			continue
		}

		// Track current schema
		if strings.HasPrefix(strings.ToUpper(line), "USE ") {
			currentSchema = strings.Trim(strings.TrimPrefix(strings.ToUpper(line), "USE "), " ;`")
			continue
		}

		buffer.WriteString(line)
		buffer.WriteString(" ")

		if strings.HasSuffix(line, ";") {
			sql := buffer.String()
			buffer.Reset()

			// Parse CREATE TABLE statements
			if matches := CreateTableRegex.FindStringSubmatch(sql); matches != nil {
				tableName := strings.Trim(matches[1], "`")
				columnDefs := matches[2]

				table := &TableDef{
					Name:    tableName,
					Schema:  currentSchema,
					Columns: parseColumns(columnDefs),
				}

				key := fmt.Sprintf("%s.%s", currentSchema, tableName)
				SchemaDefinitions[key] = table
			}
		}
	}

	return scanner.Err()
}

func parseColumns(columnDefs string) []ColumnDef {
	var columns []ColumnDef
	matches := ColumnDefRegex.FindAllStringSubmatch(columnDefs, -1)

	for _, match := range matches {
		if len(match) >= 3 && !strings.HasPrefix(strings.ToUpper(match[1]), "PRIMARY") &&
			!strings.HasPrefix(strings.ToUpper(match[1]), "KEY") &&
			!strings.HasPrefix(strings.ToUpper(match[1]), "INDEX") &&
			!strings.HasPrefix(strings.ToUpper(match[1]), "CONSTRAINT") {

			name := strings.Trim(match[1], "`")
			dataType := strings.ToUpper(match[2])
			nullable := !strings.Contains(strings.ToUpper(match[3]), "NOT NULL")

			columns = append(columns, ColumnDef{
				Name:     name,
				Type:     dataType,
				Nullable: nullable,
			})
		}
	}

	return columns
}

func GetTableDef(schema, table string) *TableDef {
	key := fmt.Sprintf("%s.%s", schema, table)
	return SchemaDefinitions[key]
}
