package datastream;

import bean.Person;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.TTLUtil;

import java.time.Duration;

public class TestTTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(Person::getName)
                .filter(new RichFilterFunction<Person>() {
                    public MapState<String, Person> mapState;
                    public MapStateDescriptor<String, Person> mapStateDescriptor;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapStateDescriptor = new MapStateDescriptor<>(
                                "broadCastDescriptor",
                                BasicTypeInfo.STRING_TYPE_INFO,
                                TypeInformation.of(new TypeHint<Person>() {
                                })
                        );
                        StateTtlConfig build = StateTtlConfig
                                .newBuilder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        mapStateDescriptor.enableTimeToLive(build);
                        System.out.println(mapStateDescriptor.getTtlConfig().getTtl().getSize());
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public boolean filter(Person value) throws Exception {

                        System.out.println(mapState.contains(value.getName()));
//                        System.out.println(mapStateDescriptor.getTtlConfig().getTtl().getSize());
                        if (!mapState.contains(value.getName())) {
                            mapState.put(value.getName(), value);
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).print();


        env.execute();
    }
}
