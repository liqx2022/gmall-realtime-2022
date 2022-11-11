package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author liqingxin
 * 2022/11/7 23:19
 * @version 1.0
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");

        // TODO 3. 读取业务主流
        String topic = "topic_db";
        String groupId = "dim_sink_app";
        DataStreamSource<String> gmallDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 4. 主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = gmallDS.map(JSON::parseObject);

        // TODO 5. 主流 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                jsonObj ->
                {
                    try {
                        jsonObj.getJSONObject("data");
                        if(jsonObj.getString("type").equals("bootstrap-start")
                                || jsonObj.getString("type").equals("bootstrap-complete")) {
                            return false;
                        }
                        return true;
                    } catch (JSONException jsonException) {
                        return false;
                    }
                });

        filterDS.print();

        env.execute();
    }
}
