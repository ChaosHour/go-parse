package schema

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
)

type Column struct {
	Name     string
	DataType string
}

type Table struct {
	Name    string
	Columns []Column
}

type Database struct {
	Name   string
	Tables map[string]*Table
}

type SchemaRegistry struct {
	Databases map[string]*Database
	warned    map[string]bool
	warnMutex sync.Mutex
	warnings  map[string]int // Track warning count per table
}

func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		Databases: make(map[string]*Database),
		warned:    make(map[string]bool),
		warnings:  make(map[string]int),
	}
}

func (sr *SchemaRegistry) LoadFromFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var currentDB *Database
	var currentTable *Table

	// Fixed regex patterns without illegal characters
	createTableRegex := regexp.MustCompile(`CREATE TABLE\s+[']?([^'\.]+)[']?\.?([^\s\(]+)`)
	columnRegex := regexp.MustCompile(`^\s*([^\s]+)\s+([^,\n]+)(?:,|$)`)

	var inCreateTable bool
	var buffer strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)

		// Skip comments and empty lines
		if strings.HasPrefix(line, "--") || strings.HasPrefix(line, "/*") || line == "" {
			continue
		}

		// Handle USE statements
		if strings.HasPrefix(strings.ToUpper(line), "USE ") {
			dbName := strings.Trim(strings.TrimPrefix(strings.ToUpper(line), "USE "), " ;`'")
			if _, exists := sr.Databases[dbName]; !exists {
				sr.Databases[dbName] = &Database{
					Name:   dbName,
					Tables: make(map[string]*Table),
				}
			}
			currentDB = sr.Databases[dbName]
			continue
		}

		// Handle CREATE TABLE
		if strings.HasPrefix(strings.ToUpper(line), "CREATE TABLE") {
			inCreateTable = true
			buffer.Reset()
			buffer.WriteString(line)
			continue
		}

		if inCreateTable {
			buffer.WriteString(" " + line)

			if strings.HasSuffix(line, ";") {
				createStmt := buffer.String()
				matches := createTableRegex.FindStringSubmatch(createStmt)
				if len(matches) > 1 {
					tableName := strings.Trim(matches[1], "`'")
					currentTable = &Table{
						Name:    tableName,
						Columns: make([]Column, 0),
					}

					// Extract column definitions
					startIdx := strings.Index(createStmt, "(")
					endIdx := strings.LastIndex(createStmt, ")")
					if startIdx > 0 && endIdx > startIdx {
						columnsPart := createStmt[startIdx+1 : endIdx]
						for _, line := range strings.Split(columnsPart, ",") {
							line = strings.TrimSpace(line)
							if matches := columnRegex.FindStringSubmatch(line); len(matches) > 2 {
								columnName := strings.Trim(matches[1], "`'")
								dataType := strings.TrimSpace(matches[2])
								if !strings.HasPrefix(strings.ToUpper(line), "PRIMARY KEY") &&
									!strings.HasPrefix(strings.ToUpper(line), "KEY") &&
									!strings.HasPrefix(strings.ToUpper(line), "UNIQUE KEY") &&
									!strings.HasPrefix(strings.ToUpper(line), "CONSTRAINT") {
									currentTable.Columns = append(currentTable.Columns, Column{
										Name:     columnName,
										DataType: dataType,
									})
								}
							}
						}
					}

					if currentDB != nil {
						currentDB.Tables[tableName] = currentTable
					}
				}
				inCreateTable = false
				buffer.Reset()
			}
		}
	}

	return scanner.Err()
}

// Add type for sorting warnings
type tableWarning struct {
	name  string
	count int
}

// Update GetTableInfo method to be completely silent
func (sr *SchemaRegistry) GetTableInfo(database, table string) *Table {
	if db, ok := sr.Databases[database]; ok {
		if tbl, ok := db.Tables[table]; ok {
			return tbl
		}
	}

	key := fmt.Sprintf("%s.%s", database, table)
	sr.warnMutex.Lock()
	sr.warnings[key]++
	sr.warnMutex.Unlock()
	return nil
}

// Update PrintWarnings to sort by reference count
func (sr *SchemaRegistry) PrintWarnings() {
	sr.warnMutex.Lock()
	defer sr.warnMutex.Unlock()

	if len(sr.warnings) == 0 {
		return
	}

	// Convert map to slice for sorting
	warnings := make([]tableWarning, 0, len(sr.warnings))
	for table, count := range sr.warnings {
		warnings = append(warnings, tableWarning{table, count})
	}

	// Sort by count (descending) then by name
	sort.Slice(warnings, func(i, j int) bool {
		if warnings[i].count != warnings[j].count {
			return warnings[i].count > warnings[j].count
		}
		return warnings[i].name < warnings[j].name
	})

	fmt.Printf("\nSchema Validation Warnings:\n")
	fmt.Printf("------------------------\n")
	for _, w := range warnings {
		fmt.Printf("Table %-40s referenced %d times\n", w.name, w.count)
	}
	fmt.Printf("\nTotal missing tables: %d\n", len(warnings))
}

// Add PrintSummary method
func (sr *SchemaRegistry) PrintSummary() {
	fmt.Println("\nSchema Registry Summary:")
	fmt.Println("=======================")
	for dbName, db := range sr.Databases {
		fmt.Printf("\nDatabase: %s\n", dbName)
		fmt.Printf("Tables: %d\n", len(db.Tables))
		for tableName, table := range db.Tables {
			fmt.Printf("  - %s (%d columns)\n", tableName, len(table.Columns))
		}
	}
	fmt.Println()
}
