package datastream;

import bean.Cluster;
import bean.Person;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author zt
 */
@Slf4j
public class TestMoreCluster {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        SingleOutputStreamOperator<String> outputStreamOperator = env.socketTextStream("localhost", 999)
                .map(data -> {
                    return data;
                });

        ArrayList<Properties> properties = new ArrayList<>();
        Properties prop1 = new Properties();
        prop1.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.1.2.248:8081,172.1.2.248:8082,172.1.2.248:8083");
        properties.add(prop1);
        Properties prop2 = new Properties();
        prop2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.1.2.245:8081,172.1.2.245:8082,172.1.2.245:8083");
        properties.add(prop2);

//        outputStreamOperator.addSink(new FlinkKafkaProducer<>("ncp-test", new SimpleStringSchema(), prop2));
        int count = 1;
        for (Properties cluster : properties) {
            count++;
            KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
                private String bootstrapservers = cluster.getProperty("bootstrap.servers");
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
                    log.info("element:{}",element);
                    Cluster cluster1 = JSON.parseObject(element, Cluster.class);
                    String url = cluster1.getUrl();
                    String topic = cluster1.getTopic();
                    log.info("url:{},topic:{},当前cluster:{},是否一致:{}",url,topic,bootstrapservers,bootstrapservers.equals(url));
                    if (bootstrapservers.equals(url)) {
                        return new ProducerRecord<>(
                                topic, // target topic
                                element.getBytes(StandardCharsets.UTF_8)); // record contents
                    }else{
                        return new ProducerRecord<>(
                                topic, // target topic
                                null);
                    }
                }
            };

            FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                    "my-topic",
                    serializationSchema,
                    cluster,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
            outputStreamOperator.addSink(myProducer).setParallelism(count);
        }
            

        env.execute();
    }

}
