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

drop table if exists dws_traffic_source_keyword_page_view_window;
create table if not exists dws_traffic_source_keyword_page_view_window
(
    `stt`           DATETIME comment '窗口起始时间',
    `edt`           DATETIME comment '窗口结束时间',
    `source`        VARCHAR(10) comment '关键词来源',
    `keyword`       VARCHAR(10) comment '关键词',
    `cur_date`      DATE comment '当天日期',
    `keyword_count` BIGINT replace comment '关键词评分'
) engine = olap
    aggregate key (`stt`, `edt`, `source`, `keyword`, `cur_date`)
comment "流量域来源-关键词粒度页面浏览汇总表"
partition by range(`cur_date`)()
distributed by hash(`keyword`) buckets 10 
properties (
  "replication_num" = "3",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-1",
  "dynamic_partition.end" = "1",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10",
  "dynamic_partition.hot_partition_num" = "1"
);

select * from gmall_realtime.dws_traffic_source_keyword_page_view_window;

create database gmall_realtime;

use gmall_realtime;