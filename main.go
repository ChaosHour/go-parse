package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	binlogFile    = flag.String("file", "", "Binlog file to parse")
	offset        = flag.Int64("offset", -1, "Starting offset (use -1 to ignore)")
	logPosition   = flag.Int64("logPosition", -1, "Log position to start from (use -1 to ignore)")
	listPositions = flag.Bool("listPositions", false, "List all log positions in the binlog")
	stopAtNext    = flag.Bool("stopAtNext", false, "Stop at the next log position")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -file <binlog file> [-offset <offset>] [-logPosition <log position>] [-listPositions] [-stopAtNext]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *binlogFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	if _, err := os.Stat(*binlogFile); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: Binlog file %s does not exist\n", *binlogFile)
		os.Exit(1)
	}

	if *listPositions {
		listAllLogPositions(*binlogFile)
		return
	}

	startPosition := *offset
	if startPosition == -1 && *logPosition != -1 {
		startPosition = *logPosition
	}

	if startPosition == -1 {
		fmt.Fprintf(os.Stderr, "Error: Either offset or log position must be specified\n")
		flag.Usage()
		os.Exit(1)
	}

	p := replication.NewBinlogParser()
	err := p.ParseFile(*binlogFile, startPosition, func(e *replication.BinlogEvent) error {
		if e.Header.LogPos >= uint32(startPosition) {
			e.Dump(os.Stdout)
			if *stopAtNext && e.Header.LogPos > uint32(startPosition) {
				return fmt.Errorf("reached log position %d", startPosition)
			}
		}
		return nil
	})

	if err != nil && err.Error() != fmt.Sprintf("Reached log position %d", startPosition) {
		fmt.Println(err.Error())
	}
}

func listAllLogPositions(binlogFile string) {
	p := replication.NewBinlogParser()
	err := p.ParseFile(binlogFile, 4, func(e *replication.BinlogEvent) error {
		fmt.Printf("Log position: %d\n", e.Header.LogPos)
		return nil
	})

	if err != nil {
		fmt.Println(err.Error())
	}
}
