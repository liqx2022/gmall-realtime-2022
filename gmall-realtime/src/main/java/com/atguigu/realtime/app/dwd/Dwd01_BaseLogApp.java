package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

/**
 * @author liqingxin
 * 2022/11/13 0:35
 */
public class Dwd01_BaseLogApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 启用状态后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies
                .failureRateRestart(10,
                        Time.of(3L, TimeUnit.DAYS),
                        Time.of(1L, TimeUnit.MINUTES)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka 读取主流数据
        String topic = "topic_log";
        String groupId = "base_log_consumer";
        DataStreamSource<String> source = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        // TODO 4. 数据清洗，转换结构
        // 4.1 定义错误侧输出流
        OutputTag<String> dirtyStreamTag = new OutputTag<String>("dirtyStream") {
        };

        // 4.2 分流（过滤脏数据），转换主流数据结构 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> cleanedStream = source.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            ctx.output(dirtyStreamTag, jsonStr);
                        }
                    }
                }
        );

        // 4.3 将脏数据写出到 Kafka 指定主题
        DataStream<String> dirtyStream = cleanedStream.getSideOutput(dirtyStreamTag);
        String dirtyTopic = "dirty_data";
        dirtyStream.addSink(KafkaUtil.getKafkaProducer(dirtyTopic));

        // TODO 5. 新老访客状态标记修复
        // 5.1 按照 mid 对数据进行分组
        KeyedStream<JSONObject, String> keyedStream = cleanedStream.keyBy(r -> r.getJSONObject("common").getString("mid"));

        // 5.2 新老访客状态标记修复
        SingleOutputStreamOperator<JSONObject> fixedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    ValueState<String> firstViewDtState;

                    @Override
                    public void open(Configuration param) throws Exception {
                        super.open(param);
                        firstViewDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                                "lastLoginDt", String.class
                        ));
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        String firstViewDt = firstViewDtState.value();
                        Long ts = jsonObj.getLong("ts");
                        String dt = DateFormatUtil.toDate(ts);

                        if ("1".equals(isNew)) {
                            if (firstViewDt == null) {
                                firstViewDtState.update(dt);
                            } else {
                                if (!firstViewDt.equals(dt)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (firstViewDt == null) {
                                // 将首次访问日期置为昨日
                                String yesterday = DateFormatUtil.toDate(ts - 1000 * 60 * 60 * 24);
                                firstViewDtState.update(yesterday);
                            }
                        }

                        out.collect(jsonObj);
                    }
                }
        );

        fixedStream.print("fixedStream");

        env.execute();
    }
}
