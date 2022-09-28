package datastream;


import bean.Person;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class TestWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ZERO)
                                .withIdleness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person element, long recordTimestamp) {
                                        return element.getTs().getTime();
                                    }
                                }))
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//                .trigger(CountTrigger.of(1))
//                .allowedLateness(Time.seconds(2))
                .process(new ProcessWindowFunction<Person, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Person, String, String, TimeWindow>.Context context, Iterable<Person> elements, Collector<String> out) throws Exception {
                        Person next = elements.iterator().next();
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long maxTimestamp = context.window().maxTimestamp();
                        long watermark = context.currentWatermark();
                        int sum = 0;

                        System.out.println(elements.toString());
                        for (Person element : elements) {
                            sum += element.getAge();
                        }
                        out.collect(sum+"-"+watermark);
                    }
                })
                .print();


        env.execute();

    }
}
