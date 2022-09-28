package functions;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Nullable;

/**
 * @author zt
 */

public class CustomKafkaSeri implements KafkaSerializationSchema {

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        KafkaSerializationSchema.super.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Object element, @Nullable Long timestamp) {
        return null;
    }
}
