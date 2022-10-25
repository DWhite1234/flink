package datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.MapUtil;

/**
 * @author zt
 */

public class TestState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }
                    @Override
                    public String map(String value) throws Exception {
                        MapUtil.map.put(value, value);
                        return value;
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        System.out.println("filter:"+MapUtil.map);
                        return true;
                    }
                })
                .print();

        env.execute();
    }
}
