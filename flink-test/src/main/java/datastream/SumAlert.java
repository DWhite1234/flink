package datastream;

import functions.JDBCsource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SumAlert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new JDBCsource())
                .print();
        env.execute();
    }
}
