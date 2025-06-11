# go-parse

A MySQL binary log parser utility.

## Usage

```Go
./go-parse  -h
Usage: cmd -file <binlog file> [-all] [-offset <offset>] [-logPosition <log position>] [-listPositions] [-stopAtNext] [-showStats] [-verbose] [-schema <schema file>]
  -all
    	Parse entire binlog file
  -file string
    	Binlog file to parse
  -listPositions
    	List all log positions in the binlog
  -logPosition int
    	Log position to start from (use -1 to ignore) (default -1)
  -offset int
    	Starting offset (use -1 to ignore) (default -1)
  -schema string
    	MySQL schema dump file to load
  -showStats
    	Show operation statistics by database and table
  -stopAtNext
    	Stop at the next log position
  -verbose
    	Show detailed position information for each event



./go-parse  -file tests/mysql-bin.000001 -listPositions
Log position: 120
Log position: 1891
Log position: 4977
Log position: 5376
Log position: 5665
Log position: 6209
Log position: 7050
Log position: 7707
Log position: 8104
Log position: 8468
Log position: 8823
Log position: 9117
Log position: 9386
Log position: 9721
Log position: 10093
Log position: 10559




./go-parse -file tests/mysql-bin.000001 -logPosition 10093 -stopAtNext
=== QueryEvent ===
Date: 2022-09-05 16:46:41
Log position: 10559
Event size: 466
Slave proxy ID: 1
Execution time: 0
Error code: 0
Schema: mysql
Query: CREATE TABLE IF NOT EXISTS time_zone_transition_type (   Time_zone_id int unsigned NOT NULL, Transition_type_id int unsigned NOT NULL, Offset int signed DEFAULT 0 NOT NULL, Is_DST tinyint unsigned DEFAULT 0 NOT NULL, Abbreviation char(8) DEFAULT '' NOT NULL, PRIMARY KEY TzIdTrTId (Time_zone_id, Transition_type_id) ) engine=MyISAM CHARACTER SET utf8   comment='Time zone transition types';

found next event at position 10559 (previous events ended at 10559)

./go-parse -file tests/mysql-bin.000012 -schema schema/sbtest-schema-only.sql -logPosition 123 -stopAtNext
=== PreviousGTIDsEvent ===
Date: 2024-11-04 21:18:19
Log position: 194
Event size: 71
Previous GTID Event: 5133d310-9498-11ef-9f1b-0242ac190003:1-11538

found next event at position 194 (previous events ended at 194)

./go-parse -file tests/mysql-bin.000012 -schema schema/sbtest-schema-only.sql -logPosition 194 -stopAtNext
=== GTIDEvent ===
Date: 2024-11-04 21:18:19
Log position: 259
Event size: 65
Commit flag: 0
GTID_NEXT: 5133d310-9498-11ef-9f1b-0242ac190003:11539
LAST_COMMITTED: 0
SEQUENCE_NUMBER: 1
Immediate commmit timestamp: 0 (<n/a>)
Orignal commmit timestamp: 0 (<n/a>)
Transaction length: 0
Immediate server version: 0
Orignal server version: 0

found next event at position 259 (previous events ended at 259)
./go-parse -file tests/mysql-bin.000012 -schema schema/sbtest-schema-only.sql -logPosition 259 -stopAtNext
=== QueryEvent ===
Date: 2024-11-04 21:18:19
Log position: 333
Event size: 74
Slave proxy ID: 8082
Execution time: 0
Error code: 0
Schema: sbtest
Query: BEGIN

found next event at position 333 (previous events ended at 333)
./go-parse -file tests/mysql-bin.000012 -schema schema/sbtest-schema-only.sql -logPosition 333 -stopAtNext
=== RowsQueryEvent ===
Date: 2024-11-04 21:18:19
Log position: 397
Event size: 64
Query: UPDATE sbtest20 SET k=k+1 WHERE id=50431

found next event at position 397 (previous events ended at 397)

 ./go-parse -file tests/mysql-bin.000001 -all
```

## Using mysqlbinlog

```bash
# List all log positions
mysqlbinlog --base64-output=DECODE-ROWS -v tests/mysql-bin.000001 | grep -E '^# at '
# at 4
# at 120
# at 1891
# at 4977
# at 5376
# at 5665
# at 6209
# at 7050
# at 7707
# at 8104
# at 8468
# at 8823
# at 9117
# at 9386
# at 9721
# at 10093
# at 10559


# Get event at position 10093
mysqlbinlog --start-position=10093 --stop-position=10559 --base64-output=DECODE-ROWS -v tests/mysql-bin.000001
# The proper term is pseudo_replica_mode, but we use this compatibility alias
# to make the statement usable on server versions 8.0.24 and older.
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 120
#220905 16:46:41 server id 168502  end_log_pos 120 CRC32 0x5884b652 	Start: binlog v 4, server v 5.6.51-91.0-log created 220905 16:46:41 at startup
ROLLBACK/*!*/;
# at 10093
#220905 16:46:41 server id 168502  end_log_pos 10559 CRC32 0xf75e8d5c 	Query	thread_id=1	exec_time=0	error_code=0
use `mysql`/*!*/;
SET TIMESTAMP=1662421601/*!*/;
SET @@session.pseudo_thread_id=1/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=0/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C utf8mb3 *//*!*/;
SET @@session.character_set_client=33,@@session.collation_connection=33,@@session.collation_server=33/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
CREATE TABLE IF NOT EXISTS time_zone_transition_type (   Time_zone_id int unsigned NOT NULL, Transition_type_id int unsigned NOT NULL, Offset int signed DEFAULT 0 NOT NULL, Is_DST tinyint unsigned DEFAULT 0 NOT NULL, Abbreviation char(8) DEFAULT '' NOT NULL, PRIMARY KEY TzIdTrTId (Time_zone_id, Transition_type_id) ) engine=MyISAM CHARACTER SET utf8   comment='Time zone transition types';
/*!*/;
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
```

## Build Instructions

Build for your current platform:
```
go build -o go-parse
```

FreeBSD:
```
env GOOS=freebsd GOARCH=amd64 go build .
```

On Mac:
```
env GOOS=darwin GOARCH=amd64 go build .
```

Linux:
```
env GOOS=linux GOARCH=amd64 go build .
```

## A Big Thank you! to [go-mysql](https://github.com/go-mysql-org/go-mysql)

You did all the hard work, and I am very grateful
