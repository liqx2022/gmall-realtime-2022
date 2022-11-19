package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TrafficPageViewBean;
import com.atguigu.realtime.function.MyDorisSink;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


/**
 * @author liqingxin
 * 2022/11/19 17:54
 */
public class Dws02_DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
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

        // TODO 3. 从 kafka dwd_traffic_page_log 主题读取页面数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_vc_ch_ar_is_new_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        // TODO 4. 转换页面流数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageLogSource.map(JSON::parseObject);

        // TODO 5. 统计会话数、页面浏览数、页面访问时长，并封装为实体类
        SingleOutputStreamOperator<TrafficPageViewBean> mainStream = jsonObjStream.map(
                new MapFunction<JSONObject, TrafficPageViewBean>() {

                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject common = jsonObj.getJSONObject("common");
                        JSONObject page = jsonObj.getJSONObject("page");

                        // 获取 ts
                        Long ts = jsonObj.getLong("ts");

                        // 获取维度信息
                        String vc = common.getString("vc");
                        String ch = common.getString("ch");
                        String ar = common.getString("ar");
                        String isNew = common.getString("is_new");

                        // 获取页面访问时长
                        Long duringTime = page.getLong("during_time");

                        // 定义变量接收其它度量值
                        Long uvCt = 0L;
                        Long svCt = 0L;
                        Long pvCt = 1L;
                        Long ujCt = 0L;

                        // 判断本页面是否开启了一个新的会话
                        String lastPageId = page.getString("last_page_id");
                        if (lastPageId == null) {
                            svCt = 1L;
                        }

                        // 封装为实体类
                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                isNew,
                                "",
                                uvCt,
                                svCt,
                                pvCt,
                                duringTime,
                                ujCt,
                                ts
                        );
                        return trafficPageViewBean;
                    }
                }
        );

        // TODO 6. 从 Kafka 读取跳出明细数据和独立访客数据，封装为流并转换数据结构，合并三条流
        // 6.1 从 Kafka dwd_traffic_user_jump_detail 读取跳出明细数据，封装为流
        String ujdTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> ujdKafkaConsumer = KafkaUtil.getKafkaConsumer(ujdTopic, groupId);
        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> ujdMappedStream =
                ujdSource.map(jsonStr -> {
                    JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts") + 10 * 1000L;

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            "",
                            0L,
                            0L,
                            0L,
                            0L,
                            1L,
                            ts
                    );
                });

        // 6.2 从 Kafka dwd_traffic_unique_visitor_detail 主题读取独立访客数据，封装为流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> uvKafkaConsumer = KafkaUtil.getKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> uvMappedStream =
                uvSource.map(jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            "",
                            1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            ts
                    );
                });

        // 6.3 合并三条流
        DataStream<TrafficPageViewBean> pageViewBeanDS = mainStream
                .union(ujdMappedStream)
                .union(uvMappedStream);


        // TODO 7. 设置水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream = pageViewBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {

                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long recordTimestamp) {
                                        return trafficPageViewBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 8. 按照维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedBeanStream = withWatermarkStream.keyBy(trafficPageViewBean ->
                        Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getIsNew()
                        )
                , Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
        );

        // TODO 9. 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedBeanStream.window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L));
        

        // TODO 10. 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reducedStream = windowStream.reduce(
                new ReduceFunction<TrafficPageViewBean>() {

                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {

                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {

                    @Override
                    public void process(Tuple4<String, String, String, String> key, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {

                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        String curDate = DateFormatUtil.toPartitionDate(context.window().getStart());
                        for (TrafficPageViewBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDate);
                            out.collect(element);
                        }
                    }
                }
        );

        reducedStream.print("reducedStream");

        // TODO 11. 写入 OLAP 数据库
        String tableName = "dws_traffic_vc_ch_ar_is_new_page_view_window";
        reducedStream.addSink(new MyDorisSink<TrafficPageViewBean>(tableName));

        env.execute();
    }
}
