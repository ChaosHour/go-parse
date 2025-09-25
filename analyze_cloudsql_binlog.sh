#! /usr/bin/env zsh

# Cloud SQL Binlog Analysis Script
# Usage: ./analyze_cloudsql_binlog.sh /path/to/mysql-bin.xxxxxx

if [ $# -eq 0 ]; then
    echo "Usage: $0 <binlog_file>"
    exit 1
fi

BINLOG_FILE="$1"
echo "🔍 Analyzing Cloud SQL Binlog: $(basename "$BINLOG_FILE")"
echo "=================================================="

echo ""
echo "📊 OPERATION SUMMARY:"
echo "-------------------"
./bin/go-parse -file "$BINLOG_FILE" -json -all | jq -r 'select(.event_type) | .event_type' | sort | uniq -c | sort -nr

echo ""
echo "🏗️  TOP SCHEMAS:"
echo "---------------"
./bin/go-parse -file "$BINLOG_FILE" -json -all | jq -r 'select(.schema) | .schema' | sort | uniq -c | sort -nr | head -5

echo ""
echo "📋 TOP TABLES (sbtest schema):"
echo "----------------------------"
./bin/go-parse -file "$BINLOG_FILE" -json -all | jq -r 'select(.schema == "sbtest" and .table) | .table' | sort | uniq -c | sort -nr | head -10

echo ""
echo "⏰ ACTIVITY TIMELINE:"
echo "-------------------"
./bin/go-parse -file "$BINLOG_FILE" -json -all | jq -r 'select(.timestamp) | .timestamp[:16]' | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔄 HIGH-CHURN TABLES (INSERT+DELETE activity):"
echo "---------------------------------------------"
./bin/go-parse -file "$BINLOG_FILE" -json -all | jq -r 'select(.schema == "sbtest" and (.event_type == "INSERT" or .event_type == "DELETE")) | .table' | sort | uniq -c | sort -nr | head -5

echo ""
echo "📈 FREQUENTLY UPDATED TABLES:"
echo "----------------------------"
./bin/go-parse -file "$BINLOG_FILE" -json -all | jq -r 'select(.schema == "sbtest" and .event_type == "UPDATE") | .table' | sort | uniq -c | sort -nr | head -5

echo ""
echo "✅ Analysis Complete!"
echo "==================="
