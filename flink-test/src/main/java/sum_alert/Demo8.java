package sum_alert;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class Demo8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 888);
        env.socketTextStream("localhost", 999)
                .keyBy(data -> data)
                .filter(new RichFilterFunction<String>() {

                    public MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, String> map = new MapStateDescriptor<>("map", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<String>() {
                        }));

                        mapState = getRuntimeContext().getMapState(map);
                    }

                    @Override
                    public boolean filter(String value) throws Exception {
                        if (mapState.contains(value)) {
                            return false;
                        } else {
                            mapState.put(value, value);
                            return true;
                        }
                    }
                }).connect(source)
                .process(new CoProcessFunction<String, String, String>() {

                    @Override
                    public void processElement1(String value, CoProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println("processElement1:" + value);
                    }

                    @Override
                    public void processElement2(String value, CoProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println("processElement2:" + value);
                    }
                }).print();

        env.execute();
}}
