package sum_alert;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Demo1 {

    private static Logger logger = LoggerFactory.getLogger(Demo1.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromCollection(Arrays.asList(Tuple2.of("ss", 2), Tuple2.of("ss", 2), Tuple2.of("ss", 3)))
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        stringIntegerTuple2.f1 = 1 + stringIntegerTuple2.f1;
                        stringIntegerTuple2.f1 = stringIntegerTuple2.f1 + t1.f1;
                        return stringIntegerTuple2;
                    }
                }).print();



        env.execute();

    }

}
