package datastream;

import bean.Person;
import bean.TestBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class AggProces {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Integer> operator = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                            @Override
                            public long extractTimestamp(Person element, long recordTimestamp) {
                                return element.getTs().getTime();
                            }
                        }))
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new AggregateFunction<Person, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Person value, Integer accumulator) {
                        return value.getAge() + accumulator;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                }, new ProcessWindowFunction<Integer, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Integer, Integer, String, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
//                        out.collect(elements.iterator().next());
                        for (Integer element : elements) {
                            out.collect(element);
                        }
                        System.out.println("start:"+context.window().getStart());
                        System.out.println("end:"+context.window().getEnd());
                    }
                });

        operator.print("minute 输出:");
        operator.windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Integer, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                        int sum = 0;
                        for (Integer element : elements) {
                            sum += element;
                            System.out.println(element);
                        }
                        System.out.println(sum);
                        out.collect(sum);
                    }
                }).print("day 输出:");

        env.execute();
    }
}
