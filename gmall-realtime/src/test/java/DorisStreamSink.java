import org.apache.doris.flink.cfg.ConfigurationOptions;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author liqingxin
 * 2022/11/16 23:35
 */
public class DorisStreamSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties prop = new Properties();
        prop.setProperty(ConfigurationOptions.DORIS_FENODES, "hadoop102:7030");
        prop.setProperty(ConfigurationOptions.DORIS_USER, "root");
        prop.setProperty(ConfigurationOptions.DORIS_PASSWORD, "000000");
        prop.setProperty(ConfigurationOptions.TABLE_IDENTIFIER, "test_db.table1");
        env.addSource(new DorisSourceFunction(new DorisStreamOptions(prop), new SimpleListDeserializationSchema())).print();
        
        env.execute();
    }
}
