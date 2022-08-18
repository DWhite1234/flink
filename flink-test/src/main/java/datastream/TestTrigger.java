package datastream;


import bean.Person;
import com.alibaba.fastjson.JSON;
import com.mysql.cj.log.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person element, long recordTimestamp) {
                                        return element.getTs().getTime();
                                    }
                                }))
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new MtTrigger())
                .process(new ProcessWindowFunction<Person, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Person, String, String, TimeWindow>.Context context, Iterable<Person> elements, Collector<String> out) throws Exception {
                        out.collect(elements.iterator().next().toString());
                    }
                }).print();

        env.execute();
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
