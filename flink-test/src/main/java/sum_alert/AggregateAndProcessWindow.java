package sum_alert;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AggregateAndProcessWindow {
    public static void main(String[] args) throws Exception {
        //1.获取流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1.数据样例
        DataStreamSource<Tuple3<Integer, Integer, Long>> datastream = env.fromElements(new Tuple3<Integer, Integer, Long>(1, 1, 1000L), new Tuple3<Integer, Integer, Long>(1, 3, 3000L), new Tuple3<Integer, Integer, Long>(1, 2, 2000L), new Tuple3<Integer, Integer, Long>(1, 7, 2000L), new Tuple3<Integer, Integer, Long>(2, 1, 4000L), new Tuple3<Integer, Integer, Long>(1, 3, 3000L), new Tuple3<Integer, Integer, Long>(1, 2, 6000L), new Tuple3<Integer, Integer, Long>(2, 2, 5000L));

        // 3.结合使用
        datastream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Integer, Integer, Long>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple3<Integer, Integer, Long> val) {
                        return val.f2;
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>, Double>() {
                    // 创建累加器
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return new Tuple2<>(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Tuple3<Integer, Integer, Long> value, Tuple2<Integer, Integer> acc) {
                        acc.f0 += 1;
                        acc.f1 += value.f1;
                        return acc;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> acc) {
                        return acc.f1*1.0;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc1, Tuple2<Integer, Integer> acc2) {
                        acc1.f0 += acc2.f0;
                        acc1.f1 += acc2.f1;
                        return acc1;
                    }

                }, new ProcessWindowFunction<Double, String, Tuple, TimeWindow>() {

                    private ValueState<Double> valueState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Double> value = new ValueStateDescriptor<>(
                                "value",
                                TypeInformation.of(new TypeHint<Double>() {})
                        );

                        valueState = getRuntimeContext().getState(value);

                    }

                    @Override
                    public void process(Tuple key, Context context, Iterable<Double> iterable, Collector<String> collector) throws Exception {
                        for (Double d : iterable) {
//                            collector.collect("key:" + key.getField(0) +",value:" + d);
                        }
                        if (valueState.value()!=null) {
                            collector.collect("总和"+iterable.iterator().next().toString());
                        }else{
                            valueState.update(iterable.iterator().next());
                        }
                        System.out.println("valueState:"+valueState.value());

                    }
                }).print();
        env.execute(AggregateAndProcessWindow.class.getSimpleName());
    }
}