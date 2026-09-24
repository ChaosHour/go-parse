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

// columnDefRegex splits a single column definition into its name and the rest
// of the definition (the data type plus any attributes).
var columnDefRegex = regexp.MustCompile(`^\s*([^\s]+)\s+(.+)$`)

// splitTopLevelCommas splits a CREATE TABLE definition list on commas that are
// not nested inside parentheses or quotes, so types such as decimal(10,2) and
// enum('a','b') survive intact.
func splitTopLevelCommas(s string) []string {
	var parts []string
	var buf strings.Builder
	depth := 0
	var quote byte

	for i := 0; i < len(s); i++ {
		c := s[i]

		if quote != 0 {
			buf.WriteByte(c)
			if c == '\\' && i+1 < len(s) {
				i++
				buf.WriteByte(s[i])
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}

		switch c {
		case '\'', '"', '`':
			quote = c
			buf.WriteByte(c)
		case '(':
			depth++
			buf.WriteByte(c)
		case ')':
			if depth > 0 {
				depth--
			}
			buf.WriteByte(c)
		case ',':
			if depth == 0 {
				parts = append(parts, buf.String())
				buf.Reset()
			} else {
				buf.WriteByte(c)
			}
		default:
			buf.WriteByte(c)
		}
	}

	if strings.TrimSpace(buf.String()) != "" {
		parts = append(parts, buf.String())
	}
	return parts
}

// tableBody returns the text between the outermost parentheses of a CREATE
// TABLE statement: the column and key definition list, without the trailing
// table options.
func tableBody(stmt string) string {
	start := strings.Index(stmt, "(")
	if start < 0 {
		return ""
	}

	depth := 0
	var quote byte
	for i := start; i < len(stmt); i++ {
		c := stmt[i]

		if quote != 0 {
			if c == '\\' && i+1 < len(stmt) {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}

		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return stmt[start+1 : i]
			}
		}
	}
	return stmt[start+1:]
}

// isKeyOrConstraint reports whether a definition is an index or constraint
// clause rather than a column.
func isKeyOrConstraint(def string) bool {
	upper := strings.ToUpper(strings.TrimSpace(def))
	for _, prefix := range []string{
		"PRIMARY KEY", "UNIQUE KEY", "UNIQUE INDEX", "UNIQUE(", "UNIQUE (",
		"FOREIGN KEY", "CONSTRAINT", "FULLTEXT", "SPATIAL", "CHECK (", "CHECK(",
		"KEY ", "KEY(", "KEY`", "INDEX ", "INDEX(", "INDEX`",
	} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

// columnsFromDefinition parses the column definitions out of a CREATE TABLE
// body. Column names are stored lowercase so lookups are case-insensitive;
// index and constraint clauses are skipped so column positions line up with
// the row images in the binlog.
func columnsFromDefinition(body string) []Column {
	cols := make([]Column, 0)
	for _, def := range splitTopLevelCommas(body) {
		def = strings.TrimSpace(def)
		if def == "" || isKeyOrConstraint(def) {
			continue
		}
		matches := columnDefRegex.FindStringSubmatch(def)
		if len(matches) < 3 {
			continue
		}
		name := strings.Trim(matches[1], "`'\"")
		if name == "" {
			continue
		}
		cols = append(cols, Column{
			Name:     strings.ToLower(name),
			DataType: strings.TrimSpace(matches[2]),
		})
	}
	return cols
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
	// mysqldump can emit a whole CREATE TABLE on one line; the default 64KiB
	// token limit would abort the scan with bufio.ErrTooLong.
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	var currentDB *Database
	var currentTable *Table
	var defaultDBName string

	// Fixed regex patterns without illegal characters
	createTableRegex := regexp.MustCompile(`CREATE TABLE\s+[']?([^'\.\s(]+)[']?(?:\.[']?([^'\s(]+)[']?)?`)

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

		// Handle USE statements (case-insensitive): switch the default
		// database for subsequent unqualified CREATE TABLE statements.
		if strings.HasPrefix(strings.ToUpper(line), "USE ") {
			dbName := line[len("USE "):]
			dbName = strings.Trim(dbName, " ;`'")
			dbName = strings.ToLower(dbName)
			if _, exists := sr.Databases[dbName]; !exists {
				sr.Databases[dbName] = &Database{
					Name:   dbName,
					Tables: make(map[string]*Table),
				}
			}
			defaultDBName = dbName
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
					currentTable.Columns = columnsFromDefinition(tableBody(createStmt))

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

// LoadFromDDL loads schema information from DDL statements (USE / CREATE TABLE).
// USE statements set the database that subsequent unqualified CREATE TABLE
// statements are attached to, so auto-discovered tables land under their real
// database name rather than a placeholder.
func (sr *SchemaRegistry) LoadFromDDL(ddlStatements []string) error {
	currentDB := ""
	for _, ddl := range ddlStatements {
		trimmed := strings.TrimSpace(ddl)
		if strings.HasPrefix(strings.ToUpper(trimmed), "USE ") {
			db := strings.Trim(trimmed[len("USE "):], " ;`'\"")
			currentDB = strings.ToLower(db)
			continue
		}
		if err := sr.parseCreateTableStatement(trimmed, currentDB); err != nil {
			// Log warning but continue with other statements. This goes to
			// stderr so it can't corrupt the JSON written to stdout.
			fmt.Fprintf(os.Stderr, "Warning: Failed to parse DDL statement: %v\n", err)
		}
	}
	return nil
}

// parseCreateTableStatement parses a single CREATE TABLE statement. Unqualified
// table names are attached to defaultDB, or to "discovered" when no USE
// statement has been seen.
func (sr *SchemaRegistry) parseCreateTableStatement(createStmt, defaultDB string) error {
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
		// just table format - use the database set by the last USE statement
		dbName = defaultDB
		if dbName == "" {
			dbName = "discovered" // fallback when no USE statement was seen
		}
		tableName = strings.Trim(matches[1], "`'\"")
	}

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
		Columns: columnsFromDefinition(tableBody(createStmt)),
	}

	// Store table (use lowercase key for lookups)
	sr.Databases[dbName].Tables[strings.ToLower(tableName)] = tbl

	return nil
}
