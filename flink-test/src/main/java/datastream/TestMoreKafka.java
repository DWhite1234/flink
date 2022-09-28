package datastream;

import bean.Person;
import com.sun.istack.internal.Nullable;
import com.sun.org.apache.bcel.internal.generic.NEW;
import functions.CustomKafkaSource;
import functions.MyMapFunction;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author zt
 */

public class TestMoreKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        ArrayList<String> list = new ArrayList<>();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(list)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka")
                .print();


        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("", new SimpleStringSchema(), new Properties());
        env.addSource(kafkaConsumer);
        env.socketTextStream("localhost", 999)
                .map(new RichMapFunction<String, String>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        list.add("demo0");
                    }

                    @Override
                    public String map(String value) throws Exception {
                        list.add(value);
                        System.out.println(list);
                        return value;
                    }
                });

        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                int i = 0;
                while (true) {
                    list.add("demo" + i);
                    i++;
                    if (i == 10) {
                        break;
                    }
                    Thread.sleep(5000);
                    System.out.println(list);
                }
            }
        }).start();


        env.execute();

    }
}
