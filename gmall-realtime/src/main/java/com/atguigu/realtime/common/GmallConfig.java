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
}
