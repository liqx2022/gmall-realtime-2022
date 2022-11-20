package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

/**
 * @author liqingxin
 * 2022/11/14 22:49
 */
public class Dwd12_DwdToolCouponOrder {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db` (\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`data` map<string, string>,\n" +
                "`type` string,\n" +
                "`old` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_order"));

        // TODO 4. 读取优惠券领用表数据，筛选满足条件的优惠券下单数据
        Table couponUseOrder = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['coupon_id'] coupon_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id,\n" +
                "data['using_time'] using_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and `type` = 'update'\n" +
                "and data['coupon_status'] = '1402'\n" +
                "and `old`['coupon_status'] = '1401'");

        tableEnv.createTemporaryView("result_table", couponUseOrder);

        // TODO 5. 建立 Kafka-Connector dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_order(\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "date_id string,\n" +
                "order_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_order"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id,\n" +
                "coupon_id,\n" +
                "user_id,\n" +
                "order_id,\n" +
                "date_id,\n" +
                "using_time order_time,\n" +
                "ts from result_table");
    }

}
