package functions;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @author zt
 */

public class CustomKafkaSource implements SourceFunction<String> {
    private static KafkaConsumer<String, String> consumer;
    private static KafkaConsumer<String, String> consumer2;
    private ArrayList<String> list = new ArrayList<>();

    static {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(prop);
        consumer2 = new KafkaConsumer<>(prop);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        StringBuilder builder = new StringBuilder();
        consumer.subscribe(Collections.singleton("demo0"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("consumer:"+record.value());
                if (record.value() != null) {
                    list.clear();
                    list.add(record.value());
                    create(record.value());
                }
            }
        }
    }

    public void create(String topic) {
        System.out.println("创建新的消费者........");
        consumer2.subscribe(list);
        while (true) {
            ConsumerRecords<String, String> records = consumer2.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(list+":" + record);
            }
        }
    }

    @Override
    public void cancel() {

    }
}
