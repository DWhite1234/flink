package datastream;

import bean.Person;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;

public class TestBroadCast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Person> broad = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class));

        SingleOutputStreamOperator<Person> broad2 = env.socketTextStream("localhost", 888)
                .map(data -> JSON.parseObject(data, Person.class));

        MapStateDescriptor<String, String> broadCast = new MapStateDescriptor<>("broadCast", String.class, String.class);
        BroadcastStream<Person> broadcastStream = broad.broadcast(broadCast);
        broad2.keyBy(Person::getName)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, Person, Person, String>() {
                    @Override
                    public void processElement(Person value, KeyedBroadcastProcessFunction<String, Person, Person, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadCast);
                        System.out.println("value:"+broadcastState.immutableEntries());
                    }

                    @Override
                    public void processBroadcastElement(Person value, KeyedBroadcastProcessFunction<String, Person, Person, String>.Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> state = ctx.getBroadcastState(broadCast);
                        state.put(value.getAge().toString(),value.toString());
                        System.out.println("broadCast:"+state.entries());
                    }
                }).print();

        broad2.keyBy(Person::getAge)
                .connect(broadcastStream)
                        .process(new KeyedBroadcastProcessFunction<Integer, Person, Person, String>() {
                            @Override
                            public void processElement(Person value, KeyedBroadcastProcessFunction<Integer, Person, Person, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                            }

                            @Override
                            public void processBroadcastElement(Person value, KeyedBroadcastProcessFunction<Integer, Person, Person, String>.Context ctx, Collector<String> out) throws Exception {
                                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadCast);
                                System.out.println("第二个广播1:"+broadcastState.entries());
                                broadcastState.put(value.getAge().toString(),value.toString());
                                System.out.println("第二个广播2:"+broadcastState.entries());
                            }
                        });
        env.execute();


    }
}
