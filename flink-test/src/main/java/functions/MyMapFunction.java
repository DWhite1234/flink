package functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.ArrayList;

/**
 * @author zt
 */

public class MyMapFunction implements MapFunction<String ,String> {
    private final ArrayList<String> list;

    public MyMapFunction(ArrayList<String> list) {
        this.list = list;
    }

    @Override
    public String map(String value) throws Exception {
        this.list.add(value);
        System.out.println(this.list);
        return list.toString();
    }
}
