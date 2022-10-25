package datastream;


import bean.Person;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Types;
import java.time.Duration;
@Slf4j
public class TestTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person element, long recordTimestamp) {
                                        return element.getTs().getTime();
                                    }
                                }))
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new MyTrigger())
                .process(new ProcessWindowFunction<Person, String, String, TimeWindow>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("初始化.....");
                    }

                    @Override
                    public void process(String s, ProcessWindowFunction<Person, String, String, TimeWindow>.Context context, Iterable<Person> elements, Collector<String> out) throws Exception {
                        System.out.println("elements:"+elements);
                    }
                });

        env.execute();
    }

    @Slf4j
    private static class MyTrigger extends Trigger<Person, TimeWindow> {
        @Override
        public TriggerResult onElement(Person element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Long> timer = ctx.getPartitionedState(new ValueStateDescriptor<Long>("timer", Long.class));
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                return TriggerResult.FIRE;
            } else {
                if (timer.value()==null) {
                    long end = window.getEnd();
                    long start = window.getStart();
                    long count = (end - start);
                    long onTime = System.currentTimeMillis() + count;
                    ctx.registerProcessingTimeTimer(onTime);
                    timer.update(onTime);
                    System.out.println();
                    System.out.printf("注册定时器:%s,当前元素:%s", onTime,element);
                }
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return time >= window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return time >= window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }
    }

    @Slf4j
    private static class MtTrigger extends Trigger<Person,TimeWindow> {
        @Override
        public TriggerResult onElement(Person element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Long> timer = ctx.getPartitionedState(new ValueStateDescriptor<Long>("timer", Long.class));
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                return TriggerResult.FIRE;
            } else {
                if (timer.value()!=null) {
                    ctx.deleteProcessingTimeTimer(timer.value());
                }
                long end = window.getEnd() / 1000 * 1000;
                long time = element.getTs().getTime();
                long count = (end - time);
                long onTime = System.currentTimeMillis() + count;
                ctx.registerProcessingTimeTimer(onTime);
                timer.update(onTime);
                return TriggerResult.CONTINUE;
            }

        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println(111);
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }
    }
}
