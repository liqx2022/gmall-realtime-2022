package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.GmallConfig;
import com.atguigu.realtime.util.DruidDSUtil;
import com.atguigu.realtime.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author liqingxin
 * 2022/11/12 16:21
 */
public class MyPhoenixSink extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建连接池
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取目标表表名
        String sinkTable = jsonObj.getString("sinkTable");
        // 获取 id 字段的值
        String id = jsonObj.getString("id");

        // 清除 JSON 对象中的 sinkTable 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sinkTable");

        // 获取字段名
        Set<String> columns = jsonObj.keySet();
        // 获取字段对应的值
        Collection<Object> values = jsonObj.values();
        // 拼接字段名
        String columnStr = StringUtils.join(columns, ",");
        // 拼接字段值
        String valueStr = StringUtils.join(values, "','");
        // 拼接插入语句
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(" +
                columnStr + ") values ('" + valueStr + "')";

        // 获取连接对象
        DruidPooledConnection conn = null;
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("从 Druid 连接池获取连接对象异常");
        }
        PhoenixUtil.executeSQL(sql, conn);
    }
}
