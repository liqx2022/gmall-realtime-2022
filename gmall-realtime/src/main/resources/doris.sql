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
