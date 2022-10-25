package datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
/**
 * @author zt
 */

public class TestMoreKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhongtao/disk/workspace/数动/ck");
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("172.1.2.245:8081,172.1.2.245:8082,172.1.2.245:8083")
                .setTopics("111")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka")
                .print();

        env.execute();

    }
}
