import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;

import java.util.Properties;

/**
 * @author liqingxin
 * 2022/11/16 23:35
 */
public class DorisStreamSinkPojo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        LogicalType[] types = {new IntType(), new SmallIntType(), new VarCharType(), new BigIntType()};
        String[] fields = {"siteid", "citycode", "username", "pv"};

        Properties prop = new Properties();
        prop.setProperty("format", "json");
        prop.setProperty("strip_outer_array", "true");
        env.fromElements(
                        new Site(19,(short)18,"night",650L)
               ).map(JSON::toJSONString)
                .addSink(DorisSink.sink(fields,types,DorisExecutionOptions.builder()
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
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Site{
        private Integer siteid;
        private Short citycode;
        private String username;
        private Long pv;
    }
}
