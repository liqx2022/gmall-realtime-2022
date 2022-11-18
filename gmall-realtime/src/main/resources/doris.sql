use test_db;

show tables;

CREATE TABLE table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
    AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");

insert into table1 values
                       (1,1,'jim',2),
                       (2,1,'grace',2),
                       (3,2,'tom',2),
                       (4,3,'bush',3),
                       (5,3,'helen',3);

show create table table1;

CREATE TABLE `table1` (
                          `siteid` int(11) NULL DEFAULT "10" COMMENT "",
                          `citycode` smallint(6) NULL COMMENT "",
                          `username` varchar(32) NULL DEFAULT "" COMMENT "",
                          `pv` bigint(20) SUM NULL DEFAULT "0" COMMENT ""
) ENGINE=OLAP
    AGGREGATE KEY(`siteid`, `citycode`, `username`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
);

select * from test_db.table1;