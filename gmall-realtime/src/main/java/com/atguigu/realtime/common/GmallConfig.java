package com.atguigu.realtime.common;

/**
 * @author liqingxin
 * 2022/11/12 16:13
 */
public class GmallConfig {

    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2022_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // Doris 数据库名
    public static final String DORIS_SCHEMA = "gmall_realtime";

    // Doris 驱动
    public static final String DORIS_DRIVER = "com.mysql.cj.jdbc.Driver";

    // Doris URL
    public static final String DORIS_URL =
            "jdbc:mysql://hadoop102:9030/gmall_realtime?user=root&password=000000";

}
