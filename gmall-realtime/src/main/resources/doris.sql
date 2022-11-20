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

show partitions from test_db.table1;

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

create table kw
(
    stt string,
    ent string,
    source string,
    keyword string,
    cut_date string,
    keyword_count bigint
)with(
    'connector'='doris',
    'fenodes'='hadoop102:7030',
    'table.identifier'='gmall_realtime.dws_traffic_source_keyword_page_view_window',
    'username'='root',
    'password'='000000',
);

show partitions from gmall_realtime.dws_traffic_source_keyword_page_view_window;

select * from gmall_realtime.dws_traffic_source_keyword_page_view_window;;

CREATE TABLE IF NOT EXISTS test_db.example_range_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
    ENGINE=OLAP
    AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
PARTITION BY RANGE(`date`)
(
    PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
    PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
    PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "3",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2023-01-01 12:00:00"
);

show partitions from test_db.example_range_tbl;

create table test_db.student_dynamic_partition1
(
    id int,
    time date,
    name varchar(50),
    age int
)
    duplicate key(id,time)
PARTITION BY RANGE(time)()
DISTRIBUTED BY HASH(id) buckets 10
PROPERTIES(
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "3",
"dynamic_partition.start" = "-7",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "10",
 "replication_num" = "1"
 );

show partitions from test_db.student_dynamic_partition1;

drop table if exists gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window;
create table if not exists gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window
(
    `stt`      DATETIME comment '窗口起始时间',
    `edt`      DATETIME comment '窗口结束时间',
    `vc`       VARCHAR(10) comment '版本',
    `ch`       VARCHAR(10) comment '渠道',
    `ar`       VARCHAR(10) comment '地区',
    `is_new`   VARCHAR(10) comment '新老访客状态标记',
    `cur_date` DATE comment '当天日期',
    `uv_ct`    BIGINT replace comment '独立访客数',
    `sv_ct`    BIGINT replace comment '会话数',
    `pv_ct`    BIGINT replace comment '页面浏览数',
    `dur_sum`  BIGINT replace comment '页面访问时长',
    `uj_ct`    BIGINT replace comment '跳出会话数'
) engine = olap aggregate key (`stt`, `edt`, `vc`, `ch`, `ar`, `is_new`, `cur_date`)
comment "流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表"
partition by range(`cur_date`)()
distributed by hash(`vc`, `ch`, `ar`, `is_new`) buckets 10 properties (
  "replication_num" = "3",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-1",
  "dynamic_partition.end" = "1",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10",
  "dynamic_partition.hot_partition_num" = "1"
);

show partitions from gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window;

select * from gmall_realtime.dws_traffic_vc_ch_ar_is_new_page_view_window;;

drop table if exists gmall_realtime.dws_traffic_page_view_window;
create table if not exists gmall_realtime.dws_traffic_page_view_window
(
    `stt`               DATETIME comment '窗口起始时间',
    `edt`               DATETIME comment '窗口结束时间',
    `cur_date`          DATE comment '当天日期',
    `home_uv_ct`        BIGINT replace comment '首页独立访客数',
    `good_detail_uv_ct` BIGINT replace comment '商品详情页独立访客数'
) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
comment "流量域页面浏览各窗口汇总表"
partition by range(`cur_date`)() 
distributed by hash(`stt`) buckets 10 properties (
  "replication_num" = "3",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-1",
  "dynamic_partition.end" = "1",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10",
  "dynamic_partition.hot_partition_num" = "1"
);

select * from gmall_realtime.dws_traffic_page_view_window;

drop table if exists gmall_realtime.dws_user_user_login_window;
create table if not exists gmall_realtime.dws_user_user_login_window
(
    `stt`      DATETIME comment '窗口起始时间',
    `edt`      DATETIME comment '窗口结束时间',
    `cur_date` DATE comment '当天日期',
    `back_ct`  BIGINT replace comment '回流用户数',
    `uu_ct`    BIGINT replace comment '独立用户数'
) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
comment "用户域用户登陆各窗口汇总表"
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10 properties (
  "replication_num" = "3",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-1",
  "dynamic_partition.end" = "1",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10",
  "dynamic_partition.hot_partition_num" = "1"
);

select * from gmall_realtime.dws_user_user_login_window;

use gmall_realtime;

show tables;

drop table if exists gmall_realtime.dws_user_user_register_window;
create table if not exists gmall_realtime.dws_user_user_register_window
(
    `stt`         DATETIME comment '窗口起始时间',
    `edt`         DATETIME comment '窗口结束时间',
    `cur_date`    DATE comment '当天日期',
    `register_ct` BIGINT replace comment '注册用户数'
) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
comment "用户域用户注册各窗口汇总表"
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10 properties (
  "replication_num" = "3",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-1",
  "dynamic_partition.end" = "1",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10",
  "dynamic_partition.hot_partition_num" = "1"
);

select * from gmall_realtime.dws_user_user_register_window;

drop table gmall_realtime.dws_trade_cart_add_uu_window;
create table if not exists gmall_realtime.dws_trade_cart_add_uu_window
(
    `stt`            DATETIME comment '窗口起始时间',
    `edt`            DATETIME comment '窗口结束时间',
    `cur_date`       DATE comment '当天日期',
    `cart_add_uu_ct` BIGINT replace comment '加购独立用户数'
) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
comment "交易域加购各窗口汇总表"
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10 properties (
  "replication_num" = "30",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-10",
  "dynamic_partition.end" = "10",
  "dynamic_partition.prefix" = "par",
  "dynamic_partition.buckets" = "10",
  "dynamic_partition.hot_partition_num" = "1"
);

show partitions from gmall_realtime.dws_user_user_register_window; 

select * from gmall_realtime.dws_user_user_register_window;