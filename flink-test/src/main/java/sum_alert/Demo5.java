package sum_alert;

import bean.Banlance;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import sun.nio.cs.StreamEncoder;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

public class Demo5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(Arrays.asList(Tuple2.of("ss", 1000), Tuple2.of("ss", 2000), Tuple2.of("ss", 3000)));
        DataStreamSource<Tuple2<String, Integer>> sourc2 = env.fromCollection(Arrays.asList(Tuple2.of("ss", 1000)));

        MapStateDescriptor<String, Tuple2<String, Integer>> checkInfoBroadCastDescriptor = new MapStateDescriptor<>(
                "checkInfoBroadCastDescriptor",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));

        MapStateDescriptor<String, Tuple2<String, Integer>> mapStateDescriptor = new MapStateDescriptor<>(
                "checkInfoBroadCastDescriptor",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));

        BroadcastStream<Tuple2<String, Integer>> broadcast = source.broadcast(checkInfoBroadCastDescriptor);
        sourc2.keyBy(data->data.f0).connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<String,Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {

                    public MapState<String, Tuple2<String, Integer>> mapState;
                    public MapState<String, Tuple2<String, Integer>> mapState2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState2 = getRuntimeContext().getMapState(mapStateDescriptor);
                        mapState = getRuntimeContext().getMapState(checkInfoBroadCastDescriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedBroadcastProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Object>.ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                        ReadOnlyBroadcastState<String, Tuple2<String, Integer>> broadcastState = ctx.getBroadcastState(checkInfoBroadCastDescriptor);


                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, Integer> value, KeyedBroadcastProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {

                        BroadcastState<String, Tuple2<String, Integer>> broadcastState = ctx.getBroadcastState(checkInfoBroadCastDescriptor);
                        broadcastState.put(value.f0,value);

                        System.out.println("2"+mapState2.keys());
                    }
                }).print();

        env.execute();
    }
}
