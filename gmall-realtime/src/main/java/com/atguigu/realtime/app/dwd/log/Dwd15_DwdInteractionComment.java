package com.atguigu.realtime.app.dwd.log;

import com.atguigu.realtime.util.KafkaUtil;
import com.atguigu.realtime.util.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liqingxin
 * 2022/11/14 22:54
 */
public class Dwd15_DwdInteractionComment {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "5 s");

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
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_comment"));

        // TODO 4. 读取评论表数据
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['appraise'] appraise,\n" +
                "proc_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'comment_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 6. 关联两张表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "ci.id,\n" +
                "ci.user_id,\n" +
                "ci.sku_id,\n" +
                "ci.order_id,\n" +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id,\n" +
                "ci.create_time,\n" +
                "ci.appraise,\n" +
                "dic.dic_name,\n" +
                "ts\n" +
                "from comment_info ci\n" +
                "join\n" +
                "base_dic for system_time as of ci.proc_time as dic\n" +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7. 建立 Kafka-Connector dwd_interaction_comment 表
        tableEnv.executeSql("create table dwd_interaction_comment(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "order_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "appraise_code string,\n" +
                "appraise_name string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_interaction_comment"));

        // TODO 8. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_comment select * from result_table");
    }

}
