package datastream;

import bean.Person;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

/**
 * @author zt
 */

public class TestListStateTTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(Person::getName)
                .map(new RichMapFunction<Person, Person>() {
                    public ListState<String> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("listState", String.class);

                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        descriptor.enableTimeToLive(ttlConfig);
                        listState = getRuntimeContext().getListState(descriptor);
                    }

                    @Override
                    public Person map(Person value) throws Exception {
                        listState.add(value.getAge()+"");
                        Iterator<String> iterator = listState.get().iterator();
                        listState.get().forEach(data->{
                            System.out.println(data);
                        });

                        return value;
                    }
                }).print();

        env.execute();
    }
}
