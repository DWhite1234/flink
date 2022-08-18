package datastream;

import bean.Person;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Slf4j
public class TestTimerTTL {
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
                .process(new KeyedProcessFunction<String, Person, String>() {
                    public MapState<String, Long> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, Long.class);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Person, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("定时器触发....1111");
                        Long time = mapState.get(ctx.getCurrentKey());
                        System.out.println("删除状态:"+mapState.entries());
                        mapState.clear();
                    }

                    @Override
                    public void processElement(Person value, KeyedProcessFunction<String, Person, String>.Context ctx, Collector<String> out) throws Exception {

                        if (!mapState.contains(ctx.getCurrentKey())) {
                            LocalDateTime dateTime = LocalDate.now().atTime(0, 0);

                            long millis = System.currentTimeMillis() + 10000;
                            ctx.timerService().registerProcessingTimeTimer(millis);
                            mapState.put(ctx.getCurrentKey(), millis);
                            System.out.println("增加定时器.....");
                        }
                        System.out.println("状态多key是否共享:{}" + ctx.getCurrentKey()+mapState.entries());
                        System.out.println("输出:" + value);

                    }
                });

        env.execute();
    }
}
