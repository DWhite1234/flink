package datastream;

import bean.Person;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zt
 */

public class TestOutput {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT,"8081");
        conf.setString(RestOptions.ADDRESS,"localhost");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        OutputTag<String> outputTag = new OutputTag<String>("demo") {
        };

        SingleOutputStreamOperator<String> process = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(data -> data.getName())
                .process(new KeyedProcessFunction<String, Person, String>() {
                    public MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, String> map = new MapStateDescriptor<>("map", String.class, String.class);
                        mapState = getRuntimeContext().getMapState(map);
                    }

                    @Override
                    public void processElement(Person value, KeyedProcessFunction<String, Person, String>.Context ctx, Collector<String> out) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        String currentKey = ctx.getCurrentKey();
                        mapState.put(currentKey, value.toString());
                        System.out.println("当前的subtask:" + indexOfThisSubtask + ",当前的key:" + currentKey + ",当前的状态:" + mapState.entries());
                        if (value.getName().equals("zs")) {
                            ctx.output(outputTag, value.toString());
                        } else {
                            out.collect(value.toString());
                        }
                    }
                });

        process.print();
        DataStream<String> sideOutput = process.getSideOutput(outputTag);
        process.union(sideOutput);


        env.execute();
    }
}
