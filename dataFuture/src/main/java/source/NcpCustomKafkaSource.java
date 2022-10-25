package source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import pojo.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zt
 */
@Slf4j
public class NcpCustomKafkaSource extends RichSourceFunction<Envelope<JSONObject>> {
    public static KafkaConsumer<String, String> topicChangeListener;
    public static KafkaProducer<String, String> topicChangeProducer;
    public static KafkaConsumer<String, String> infoListener;
    public String topicChange;
    public ThreadPoolExecutor threadPoolExecutor;
    //主题变更集合
    public List<String> sourceTopics = new ArrayList<>();
    //源主题详情信息
    public List<TopicGroup> sourceTopicsDetail;
    //目标主题详情信息
    public Map<String, Topic> targetTopicsDetail;
    //投递流程
    public List<DeliveryProcess> deliveryProcesses;
    //uuid
    public String uuid;


    @Override
    public void open(Configuration parameters) throws Exception {
//        topicChange = config.get(ConfigParameters.TOPIC_CHANGE);
//        topicChangeListener = KafkaConfig.initTopicChangeSub(config);
//        topicChangeProducer = KafkaConfig.initTopicChangePublish(config);
//        threadPoolExecutor = ThreadPoolConfig.initThreadPool(config);
    }

    @Override
    public void run(SourceContext<Envelope<JSONObject>> ctx) throws Exception {
        Tuple3<String, Integer, Long> tuple3 = getTuple2(topicChange);
        TopicPartition topicPartition = new TopicPartition(topicChange, tuple3.f1);
        topicChangeListener.assign(Arrays.asList(topicPartition));
        topicChangeListener.seek(topicPartition,tuple3.f2);
        //监听主题配置
        while (true) {
            ConsumerRecords<String, String> topicChangeRecords = topicChangeListener.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> topicChangeRecord : topicChangeRecords) {
                try {
                    JSONObject jsonObject = JSON.parseObject(topicChangeRecord.value());
                    Envelope.Head head = JSON.parseObject(jsonObject.get("head").toString(), Envelope.Head.class);
                    if (head != null &&
                            (TransactionCode.POSTMAN_INFO_RESPONSE.equals(head.getTransactionType())&&head.getMsgSeqId().equals(uuid))||
                            (TransactionCode.POST_MODIFY.equals(head.getTransactionType()))) {
//                        Postman postman = JSON.parseObject(jsonObject.get("body").toString(), Postman.class);
                        log.info("主题变更消息:{}",jsonObject.get("body"));
                        //主题变更,清除监听的主题
                        if (sourceTopics!=null) {
                            sourceTopics.clear();
                        }
                        //主题变更,清除投递流程
                        if (deliveryProcesses!=null) {
                            deliveryProcesses.clear();
                        }
                        //获取源主题详情
//                        sourceTopicsDetail = postman.getSourceTopics();
                        //获取目标主题详情
//                        targetTopicsDetail = postman.getTargetTopics();
//                        if (postman.getDeliveryProcesses() != null) {
//                            deliveryProcesses = postman.getDeliveryProcesses();
                            //获取所有需要监听的源主题
                            for (DeliveryProcess deliveryProcess : deliveryProcesses) {
                                sourceTopics.addAll(deliveryProcess.getSourceTopicCodes());
                            }
                            //监听源主题
                            consumerInfo(ctx);
                    }
                } catch (Exception e) {
                    log.info("未收到主题信息....");
                }
                
            }

        }
    }
    public void consumerInfo(SourceContext<Envelope<JSONObject>> ctx) {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                log.info("当前监听的主题列表:{}",sourceTopics);
//                infoListener = KafkaConfig.initKafkaConsumer(config);
                infoListener.subscribe(sourceTopics);
                while (true) {
                    ConsumerRecords<String, String> records = infoListener.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        JSONObject jsonObject = JSON.parseObject(record.value());
                        Envelope.Head head = JSON.parseObject(jsonObject.get("head").toString(), Envelope.Head.class);
                        //每条消息,带上投递流程,根据投递流程,获取过滤规则等
                        head.setDeliveryProcesses(deliveryProcesses);
                        head.setSourceTopicsDetails(sourceTopicsDetail);
                        head.setTargetTopicsDetails(targetTopicsDetail);
                        JSONObject object = JSON.parseObject(jsonObject.get("body").toString());
                        Envelope<JSONObject> objectEnvelope = new Envelope<>();
                        objectEnvelope.setHead(head);
                        objectEnvelope.setBody(object);
                        log.info("数据发送:{}",objectEnvelope);
                        ctx.collect(objectEnvelope);
                    }
                }
            }
        });
    }

    @Override
    public void cancel() {

    }

    public Tuple3<String, Integer, Long> getTuple2(String topic)  {
        //请求当前的主题配置
        Envelope.Head head = new Envelope.Head();
        String uuid = UUID.randomUUID().toString().replace("-", "");
        if (uuid.length() > 36) {
            uuid = uuid.substring(0, 36);
        }
        this.uuid = uuid;
        head.setMsgSeqId(uuid);
//        head.setTransactionType(config.get(ConfigParameters.TRANSACTION_TYPE));
//        head.setTransactionCode(config.get(ConfigParameters.TRANSACTION_CODE));
        Envelope<JSON> envelope = new Envelope<JSON>();
//        envelope.setHead(head);
//        envelope.setBody(null);
        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(ConfigParameters.BOOTSTRAP_SERVERS));
        Tuple3<String, Integer, Long> tuple3 = new Tuple3<>();
        Future<RecordMetadata> recordMetadataFuture = topicChangeProducer.send(new ProducerRecord<>(topic, JSON.toJSONString(envelope)));
        try {
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            int partition = recordMetadata.partition();
            long offset = recordMetadata.offset();
            tuple3.f0 = uuid;
            tuple3.f1 = partition;
            tuple3.f2 = offset;
            log.info("登录消息-uuid:{},offset:{},partition:{}",this.uuid,offset,partition);
            return tuple3;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
