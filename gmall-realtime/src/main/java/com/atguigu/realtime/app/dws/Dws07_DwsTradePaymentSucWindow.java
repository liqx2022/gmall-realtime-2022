package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TradePaymentWindowBean;
import com.atguigu.realtime.function.MyDorisSink;
import com.atguigu.realtime.util.DateFormatUtil;
import com.atguigu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @author liqingxin
 * 2022/11/20 16:05
 */
public class Dws07_DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        // 需要启动 对我的05、dwd06、dwd08
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

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

        // TODO 3. 从 Kafka dwd_trade_pay_detail_suc 主题读取支付成功明细数据，封装为流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        // 经过两次 keyedProcessFunction 处理之后开窗，数据的时间语义会发生紊乱，可能会导致数据无法进入正确的窗口
        // 因此使用处理时间去重，在分组统计之前设置一次水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkSecondStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        // TODO 6. 按照用户 id 分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkSecondStream.keyBy(r -> r.getString("user_id"));

        // TODO 7. 统计独立支付人数和新增支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> paymentWindowBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {

                    private ValueState<String> lastPaySucDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastPaySucDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_pay_suc_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TradePaymentWindowBean> out) throws Exception {
                        String lastPaySucDt = lastPaySucDtState.value();
                        Long ts = jsonObj.getLong("ts") * 1000;
                        String paySucDt = DateFormatUtil.toDate(ts);

                        Long paymentSucUniqueUserCount = 0L;
                        Long paymentSucNewUserCount = 0L;

                        if (lastPaySucDt == null) {
                            paymentSucUniqueUserCount = 1L;
                            paymentSucNewUserCount = 1L;
                        } else {
                            if (!lastPaySucDt.equals(paySucDt)) {
                                paymentSucUniqueUserCount = 1L;
                            }
                        }
                        lastPaySucDtState.update(paySucDt);

                        TradePaymentWindowBean tradePaymentWindowBean = new TradePaymentWindowBean(
                                "",
                                "",
                                "",
                                paymentSucUniqueUserCount,
                                paymentSucNewUserCount
                        );

                        long currentWatermark = ctx.timerService().currentWatermark();
                        out.collect(tradePaymentWindowBean);
                    }
                }
        );

        // TODO 8. 开窗
        AllWindowedStream<TradePaymentWindowBean, TimeWindow> windowDS = paymentWindowBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> aggregatedDS = windowDS
                .aggregate(
                        new AggregateFunction<TradePaymentWindowBean, TradePaymentWindowBean, TradePaymentWindowBean>() {
                            @Override
                            public TradePaymentWindowBean createAccumulator() {
                                return new TradePaymentWindowBean(
                                        "",
                                        "",
                                        "",
                                        0L,
                                        0L
                                );
                            }

                            @Override
                            public TradePaymentWindowBean add(TradePaymentWindowBean value, TradePaymentWindowBean accumulator) {
                                accumulator.setPaymentSucUniqueUserCount(
                                        accumulator.getPaymentSucUniqueUserCount() + value.getPaymentSucUniqueUserCount()
                                );
                                accumulator.setPaymentSucNewUserCount(
                                        accumulator.getPaymentSucNewUserCount() + value.getPaymentSucNewUserCount()
                                );
                                return accumulator;
                            }

                            @Override
                            public TradePaymentWindowBean getResult(TradePaymentWindowBean accumulator) {
                                return accumulator;
                            }

                            @Override
                            public TradePaymentWindowBean merge(TradePaymentWindowBean a, TradePaymentWindowBean b) {
                                return null;
                            }
                        },

                        new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<TradePaymentWindowBean> elements, Collector<TradePaymentWindowBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                String curDate = DateFormatUtil.toPartitionDate(context.window().getStart());
                                for (TradePaymentWindowBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setCurDate(curDate);
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 10. 写出到 OLAP 数据库
        String tableName = "dws_trade_payment_suc_window";
        aggregatedDS.addSink(new MyDorisSink<TradePaymentWindowBean>(tableName));

        env.execute();
    }

}
