#! /usr/bin/env zsh

echo " FUZZY SEARCH DEMONSTRATION"
echo "============================="
echo ""

echo " Your Cloud SQL Binlog (Row-Based):"
echo "------------------------------------"
echo "Available queries: Only transaction boundaries"
echo ""
./bin/go-parse -file mysql-bin.004258 -fuzzySearch -searchKeywords "BEGIN" -json -all | jq 'select(.matched_keyword) | {keyword: .matched_keyword, query: .query, timestamp: .timestamp}' | head -2

echo ""
echo " What you CAN search for in Cloud SQL:"
echo "---------------------------------------"
echo "• BEGIN (transaction start)"
echo "• COMMIT (transaction end)"  
echo "• ROLLBACK (transaction rollback)"
echo ""

echo " Statement-Based Binlog Example:"
echo "----------------------------------"
echo "Full SQL queries with fuzzy search:"
echo ""
./bin/go-parse -file tests/mysql-bin.000001 -fuzzySearch -searchKeywords "CREATE" -json -all | jq 'select(.matched_keyword) | {keyword: .matched_keyword, query: (.query | .[0:120] + "..."), schema: .schema}' | head -2

echo ""
echo " What you COULD search for with statement-based:"
echo "------------------------------------------------"
echo "• SELECT statements"
echo "• INSERT statements" 
echo "• UPDATE statements"
echo "• DELETE statements"
echo "• CREATE TABLE/INDEX"
echo "• ALTER TABLE"
echo "• DROP statements"
echo "• Any custom SQL patterns"
echo ""

echo " For Cloud SQL users who need SQL analysis:"
echo "--------------------------------------------"
echo "1. Enable General Query Log (if available)"
echo "2. Use application-level SQL logging"
echo "3. Implement database proxy with query logging"
echo "4. Use Cloud SQL audit logs for DDL tracking"
