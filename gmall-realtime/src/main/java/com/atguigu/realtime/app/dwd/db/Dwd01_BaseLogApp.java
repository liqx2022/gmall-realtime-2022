package com.atguigu.realtime.app.dwd.db;

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

        // TODO 6. 分流
        // 6.1 定义启动、曝光、动作、错误侧输出流
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("errorTag") {
        };

        // 6.2 分流
        SingleOutputStreamOperator<String> separatedStream = fixedStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<String> out) throws Exception {

                        // 6.2.1 收集错误数据
                        JSONObject error = jsonObj.getJSONObject("err");
                        if (error != null) {
                            context.output(errorTag, jsonObj.toJSONString());
                        }

                        // 剔除 "err" 字段
                        jsonObj.remove("err");

                        // 6.2.2 收集启动数据
                        JSONObject start = jsonObj.getJSONObject("start");
                        if (start != null) {
                            context.output(startTag, jsonObj.toJSONString());
                        } else {
                            // 获取 "page" 字段
                            JSONObject page = jsonObj.getJSONObject("page");
                            // 获取 "common" 字段
                            JSONObject common = jsonObj.getJSONObject("common");
                            // 获取 "ts"
                            Long ts = jsonObj.getLong("ts");

                            // 6.2.3 收集曝光数据
                            JSONArray displays = jsonObj.getJSONArray("displays");
                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    JSONObject displayObj = new JSONObject();
                                    displayObj.put("display", display);
                                    displayObj.put("common", common);
                                    displayObj.put("page", page);
                                    displayObj.put("ts", ts);
                                    context.output(displayTag, displayObj.toJSONString());
                                }
                            }

                            // 6.2.4 收集动作数据
                            JSONArray actions = jsonObj.getJSONArray("actions");
                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject action = actions.getJSONObject(i);
                                    JSONObject actionObj = new JSONObject();
                                    actionObj.put("action", action);
                                    actionObj.put("common", common);
                                    actionObj.put("page", page);
                                    context.output(actionTag, actionObj.toJSONString());
                                }
                            }

                            // 6.2.5 收集页面数据
                            jsonObj.remove("displays");
                            jsonObj.remove("actions");
                            out.collect(jsonObj.toJSONString());
                        }

                    }
                }
        );

//        // 打印主流和各侧输出流查看分流效果
//        separatedStream.print("page>>>");
//        separatedStream.getSideOutput(startTag).print("start!!!");
//        separatedStream.getSideOutput(displayTag).print("display@@@");
//        separatedStream.getSideOutput(actionTag).print("action###");
//        separatedStream.getSideOutput(errorTag).print("error$$$");

        // TODO 7. 将数据输出到 Kafka 的不同主题
        // 7.1 提取各侧输出流
        DataStream<String> startDS = separatedStream.getSideOutput(startTag);
        DataStream<String> displayDS = separatedStream.getSideOutput(displayTag);
        DataStream<String> actionDS = separatedStream.getSideOutput(actionTag);
        DataStream<String> errorDS = separatedStream.getSideOutput(errorTag);

        // 7.2 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        separatedStream.addSink(KafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(KafkaUtil.getKafkaProducer(start_topic));
        displayDS.addSink(KafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(KafkaUtil.getKafkaProducer(action_topic));
        errorDS.addSink(KafkaUtil.getKafkaProducer(error_topic));


        env.execute();
    }
}
