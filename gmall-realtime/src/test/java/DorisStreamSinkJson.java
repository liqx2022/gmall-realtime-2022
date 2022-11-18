import org.apache.doris.flink.cfg.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author liqingxin
 * 2022/11/16 23:35
 */
public class DorisStreamSinkJson {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties prop = new Properties();
        prop.setProperty("format", "json");
        prop.setProperty("strip_outer_array", "true");
        env.fromElements(
                        "{\"siteid\": \"10\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}")
                .addSink(DorisSink.sink(DorisExecutionOptions.builder()
                                .setBatchIntervalMs(2000L)
                                .setEnableDelete(false)
                                .setMaxRetries(3)
                                .setStreamLoadProp(prop)
                                .build(),

                        DorisOptions.builder()
                                .setFenodes("hadoop102:7030")
                                .setUsername("root")
                                .setPassword("000000")
                                .setTableIdentifier("test_db.table1").build()));
        env.execute();
    }
}
