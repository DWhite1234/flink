package sum_alert;

import bean.Banlance;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.protocol.types.Field;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class Demo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        MapStateDescriptor<String, String> map = new MapStateDescriptor<>(
                "map",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {
                })
        );
        env.fromElements(new Banlance("1", "3", "3", "3", new BigDecimal(100), "2022-05-06 12:23:22", new Timestamp(new Date().getTime()), new Date()))
                .keyBy(data -> data.account_Balance_Id)
                .map(new RichMapFunction<Banlance, Banlance>() {

                    public MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, String> map = new MapStateDescriptor<>(
                                "map",
                                BasicTypeInfo.STRING_TYPE_INFO,
                                TypeInformation.of(new TypeHint<String>() {
                                })
                        );
                        mapState = getRuntimeContext().getMapState(map);
                    }

                    @Override
                    public Banlance map(Banlance banlance) throws Exception {
                        String s = mapState.get("1");
                        if (s != null) {
                            System.out.println("!null");
                        } else {
                            System.out.println("null");
                        }
                        return banlance;
                    }
                }).print();
        env.execute();

    }
}
