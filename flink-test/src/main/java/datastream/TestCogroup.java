package datastream;


import bean.Person;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TestCogroup {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Person> ds1 = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                            @Override
                            public long extractTimestamp(Person element, long recordTimestamp) {
                                return element.getTs().getTime();
                            }
                        }));

        SingleOutputStreamOperator<Person> ds2 = env.socketTextStream("localhost", 888)
                .map(data -> JSON.parseObject(data, Person.class));

        ds1.coGroup(ds2)
                .where(Person::getName)
                .equalTo(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new CoGroupFunction<Person, Person, Object>() {
                    @Override
                    public void coGroup(Iterable<Person> first, Iterable<Person> second, Collector<Object> out) throws Exception {

                    }
                })
                .print();

        env.execute();

    }
}
