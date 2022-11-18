import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;


import java.util.Properties;

/**
 * @author liqingxin
 * 2022/11/16 23:35
 */
public class DorisStreamSinkRowData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        LogicalType[] types = {new IntType(), new SmallIntType(), new VarCharType(), new BigIntType()};
        String[] fields = {"siteid", "citycode", "username", "pv"};

        Properties prop = new Properties();
        prop.setProperty("format", "json");
        prop.setProperty("strip_outer_array", "true");
        env.fromElements(
                        "{\"siteid\": \"100\", \"citycode\": \"1002\", \"username\": \"lqx\",\"pv\": \"101\"}")
                .map(str->{
                    JSONObject obj = JSON.parseObject(str);
                    GenericRowData rowData = new GenericRowData(4);
                    rowData.setField(0,obj.getIntValue("siteid"));
                    rowData.setField(1,obj.getShortValue("citycode"));
                    rowData.setField(2, StringData.fromString(obj.getString("username")));
                    rowData.setField(3,obj.getLongValue("pv"));
                    return rowData;

                })
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
}
