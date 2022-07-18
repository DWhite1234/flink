package sum_alert;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class Demo2 {

    private static Logger logger = LoggerFactory.getLogger(Demo2.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromCollection(Arrays.asList(Tuple2.of("ss", 1000), Tuple2.of("ss", 2000), Tuple2.of("ss", 3000)))

                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Integer> t, long l) {
                                return t.f1;
                            }
                        }))
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                               @Override
                               public Integer createAccumulator() {
                                   return 0;
                               }

                               @Override
                               public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
                                   return integer + stringIntegerTuple2.f1;
                               }

                               @Override
                               public Integer getResult(Integer integer) {
                                   return integer;
                               }

                               @Override
                               public Integer merge(Integer integer, Integer acc1) {
                                   return integer + acc1;
                               }
                           },
                        new ProcessWindowFunction<Integer, Integer, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<Integer, Integer, String, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                                System.out.println("key:" + s);
                                for (Integer element : elements) {
                                    out.collect(element);
                                }
                            }
                        }).print();



        env.execute();

    }

}
