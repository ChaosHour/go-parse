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
	var defaultDBName string

	// Fixed regex patterns without illegal characters
	createTableRegex := regexp.MustCompile(`CREATE TABLE\s+[']?([^'\.\s(]+)[']?(?:\.[']?([^'\s(]+)[']?)?`)
	columnRegex := regexp.MustCompile(`^\s*([^\s]+)\s+([^,\n]+)(?:,|$)`)

	var inCreateTable bool
	var buffer strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)

		// Extract database name from header comment
		if strings.Contains(line, "Database:") {
			parts := strings.Split(line, "Database:")
			if len(parts) > 1 {
				defaultDBName = strings.ToLower(strings.TrimSpace(parts[1]))
			}
		}

		// Skip comments and empty lines
		if strings.HasPrefix(line, "--") || strings.HasPrefix(line, "/*") || line == "" {
			continue
		}

		// Handle USE statements (case-insensitive)
		if strings.HasPrefix(strings.ToUpper(line), "USE ") {
			dbName := strings.Trim(strings.TrimPrefix(line, line[:4]), " ;`'")
			dbName = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "USE"), "use"))
			dbName = strings.Trim(dbName, " ;`'")
			dbName = strings.ToLower(dbName)
			if _, exists := sr.Databases[dbName]; !exists {
				sr.Databases[dbName] = &Database{
					Name:   dbName,
					Tables: make(map[string]*Table),
				}
			}
			currentDB = sr.Databases[dbName]
			continue
		}

		// Skip DROP TABLE lines (e.g. "DROP TABLE IF EXISTS `BotCommand`;")
		if strings.HasPrefix(strings.ToUpper(line), "DROP TABLE") {
			continue
		}

		// Handle CREATE TABLE (may include database qualifier)
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
				if len(matches) >= 2 {
					var dbName, tableName string

					if len(matches) > 2 && matches[2] != "" {
						// database.table format
						dbName = strings.ToLower(strings.Trim(matches[1], "`'"))
						tableName = strings.Trim(matches[2], "`'")
					} else {
						// just table format - use default database from header
						if defaultDBName != "" {
							dbName = defaultDBName
						} else {
							dbName = "" // fallback to empty string
						}
						tableName = strings.Trim(matches[1], "`'")
					}

					// normalize table name to lowercase for lookups but keep original in struct
					tbl := &Table{
						Name:    tableName,
						Columns: make([]Column, 0),
					}

					// Ensure we have a database to attach to
					if dbName != "" {
						if _, exists := sr.Databases[dbName]; !exists {
							sr.Databases[dbName] = &Database{
								Name:   dbName,
								Tables: make(map[string]*Table),
							}
						}
						sr.Databases[dbName].Tables[strings.ToLower(tableName)] = tbl
						currentDB = sr.Databases[dbName]
					} else {
						// Use default database from header or empty string
						if defaultDBName != "" {
							dbName = defaultDBName
							if _, exists := sr.Databases[dbName]; !exists {
								sr.Databases[dbName] = &Database{
									Name:   dbName,
									Tables: make(map[string]*Table),
								}
							}
							sr.Databases[dbName].Tables[strings.ToLower(tableName)] = tbl
							currentDB = sr.Databases[dbName]
						} else {
							// Fallback to empty string database
							if _, exists := sr.Databases[""]; !exists {
								sr.Databases[""] = &Database{
									Name:   "",
									Tables: make(map[string]*Table),
								}
							}
							sr.Databases[""].Tables[strings.ToLower(tableName)] = tbl
							currentDB = sr.Databases[""]
						}
					}
					currentTable = tbl

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
									// normalize column name storage for case-insensitive lookup
									currentTable.Columns = append(currentTable.Columns, Column{
										Name:     strings.ToLower(columnName),
										DataType: dataType,
									})
								}
							}
						}
					}

					if currentDB != nil {
						// ensure stored key is lowercase for consistent lookups
						currentDB.Tables[strings.ToLower(currentTable.Name)] = currentTable
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
	dbKey := strings.ToLower(database)
	tblKey := strings.ToLower(table)
	if db, ok := sr.Databases[dbKey]; ok {
		if tbl, ok := db.Tables[tblKey]; ok {
			return tbl
		}
	}

	key := fmt.Sprintf("%s.%s", dbKey, tblKey)
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

// LoadFromDDL loads schema information from DDL statements (CREATE TABLE)
func (sr *SchemaRegistry) LoadFromDDL(ddlStatements []string) error {
	for _, ddl := range ddlStatements {
		if err := sr.parseCreateTableStatement(ddl); err != nil {
			// Log warning but continue with other statements
			fmt.Printf("Warning: Failed to parse DDL statement: %v\n", err)
		}
	}
	return nil
}

// parseCreateTableStatement parses a single CREATE TABLE statement
func (sr *SchemaRegistry) parseCreateTableStatement(createStmt string) error {
	// Clean up the statement
	createStmt = strings.TrimSpace(createStmt)
	if !strings.HasPrefix(strings.ToUpper(createStmt), "CREATE TABLE") {
		return fmt.Errorf("not a CREATE TABLE statement")
	}

	// Extract table name and database
	createTableRegex := regexp.MustCompile(`CREATE TABLE\s+['"]?([^'"\.\s(]+)['"]?(?:\.['"]?([^'"\s(]+)['"]?)?`)
	matches := createTableRegex.FindStringSubmatch(createStmt)
	if len(matches) < 2 {
		return fmt.Errorf("could not extract table name from CREATE TABLE statement")
	}

	var dbName, tableName string
	if len(matches) > 2 && matches[2] != "" {
		// database.table format
		dbName = strings.ToLower(strings.Trim(matches[1], "`'\""))
		tableName = strings.Trim(matches[2], "`'\"")
	} else {
		// just table format - assume default database
		dbName = "discovered" // default database name for autodiscovered schemas
		tableName = strings.Trim(matches[1], "`'\"")
	}

	// Extract column definitions
	columnRegex := regexp.MustCompile(`\s*['"]?([^'"\s]+)['"]?\s+([^,;\n]+)(?:,|$)`)
	columnMatches := columnRegex.FindAllStringSubmatch(createStmt, -1)

	// Ensure database exists
	if _, exists := sr.Databases[dbName]; !exists {
		sr.Databases[dbName] = &Database{
			Name:   dbName,
			Tables: make(map[string]*Table),
		}
	}

	// Create table
	tbl := &Table{
		Name:    tableName,
		Columns: make([]Column, 0, len(columnMatches)),
	}

	// Parse columns
	for _, match := range columnMatches {
		if len(match) >= 3 {
			colName := strings.Trim(match[1], "`'\"")
			colType := strings.TrimSpace(match[2])

			// Skip if this looks like a constraint or key definition
			if strings.Contains(strings.ToUpper(colType), "PRIMARY KEY") ||
				strings.Contains(strings.ToUpper(colType), "KEY") ||
				strings.Contains(strings.ToUpper(colType), "CONSTRAINT") ||
				strings.Contains(strings.ToUpper(colType), "INDEX") {
				continue
			}

			tbl.Columns = append(tbl.Columns, Column{
				Name:     colName,
				DataType: colType,
			})
		}
	}

	// Store table (use lowercase key for lookups)
	sr.Databases[dbName].Tables[strings.ToLower(tableName)] = tbl

	return nil
}
