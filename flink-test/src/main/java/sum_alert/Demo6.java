package sum_alert;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.forBoundedOutOfOrderness;

public class Demo6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> source = env.fromCollection(Arrays.asList(Tuple2.of("ss", "2022-07-12 12:12:23"), Tuple2.of("ss", "2022-07-12 12:12:25"), Tuple2.of("ss", "2022-07-12 12:12:26"),Tuple2.of("ss", "2022-07-13 12:12:26")));
        source
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, String> element, long recordTimestamp) {
                                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        Date parse = null;
                                        try {
                                             parse = simpleDateFormat.parse(element.f1);
                                        } catch (ParseException e) {
                                            throw new RuntimeException(e);
                                        }
                                        return parse.getTime();
                                    }
                                }))
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .process(new ProcessWindowFunction<Tuple2<String, String>, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, String>, Object, String, TimeWindow>.Context context, Iterable<Tuple2<String, String>> elements, Collector<Object> out) throws Exception {
                        int a = 0;
                        for (Tuple2<String, String> element : elements) {
                            a++;
                        }
                        System.out.println("start:"+context.window().getStart());
                        System.out.println("end:"+context.window().getEnd());
                        out.collect(a);
                    }
                }).print();

        env.execute();
}}
