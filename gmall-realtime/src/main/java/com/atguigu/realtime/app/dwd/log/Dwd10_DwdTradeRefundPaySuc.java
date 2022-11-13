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
 * 2022/11/14 0:43
 */
public class Dwd10_DwdTradeRefundPaySuc {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "5 s");

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
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

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` map<string, string>,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_refund_pay_suc"));

        // TODO 4. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 5. 读取退款表数据，并筛选退款成功数据
        Table refundPayment = tableEnv.sqlQuery("select\n" +
                        "data['id'] id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "data['total_amount'] total_amount,\n" +
                        "proc_time,\n" +
                        "ts\n" +
                        "from topic_db\n" +
                        "where `table` = 'refund_payment'\n" +
//                "and `type` = 'update'\n" +
                        "and data['refund_status'] = '0702'\n"
//                        +
//                "and `old`['refund_status'] is not null"
        );

        tableEnv.createTemporaryView("refund_payment", refundPayment);

        // TODO 6. 读取订单表数据并过滤退款成功订单数据
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['province_id'] province_id,\n" +
                        "`old`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_info'\n" +
                        "and `type` = 'update'\n"
//                +
//                "and data['order_status']='1006'\n" +
//                "and `old`['order_status'] is not null"
        );

        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 7. 读取退单表数据并过滤退款成功数据
        Table orderRefundInfo = tableEnv.sqlQuery("select\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['refund_num'] refund_num,\n" +
                        "`old`\n" +
                        "from topic_db\n" +
                        "where `table` = 'order_refund_info'\n"
//                        +
//                        "and `type` = 'update'\n" +
//                        "and data['refund_status']='0705'\n" +
//                        "and `old`['refund_status'] is not null"
                // order_refund_info 表的 refund_status 字段值均为 null
        );

        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 8. 关联四张表获得退款成功表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "rp.id,\n" +
                "oi.user_id,\n" +
                "rp.order_id,\n" +
                "rp.sku_id,\n" +
                "oi.province_id,\n" +
                "rp.payment_type,\n" +
                "dic.dic_name payment_type_name,\n" +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id,\n" +
                "rp.callback_time,\n" +
                "ri.refund_num,\n" +
                "rp.total_amount,\n" +
                "rp.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from refund_payment rp \n" +
                "join \n" +
                "order_info oi\n" +
                "on rp.order_id = oi.id\n" +
                "join\n" +
                "order_refund_info ri\n" +
                "on rp.order_id = ri.order_id\n" +
                "and rp.sku_id = ri.sku_id\n" +
                "join \n" +
                "base_dic for system_time as of rp.proc_time as dic\n" +
                "on rp.payment_type = dic.dic_code\n");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 9. 创建 Kafka-Connector dwd_trade_refund_pay_suc 表
        tableEnv.executeSql("create table dwd_trade_refund_pay_suc(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "date_id string,\n" +
                "callback_time string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_refund_pay_suc")); // 如果没数据出来可能是造数据的问题，程序是没有问题的

        // TODO 10. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_refund_pay_suc select * from result_table");
    }

}
