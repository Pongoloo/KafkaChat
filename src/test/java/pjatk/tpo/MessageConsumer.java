package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MessageConsumer{
    KafkaConsumer<String,String> kafkaConsumer;
     Map<TopicPartition, Long> userOffsets = new HashMap<>();

    public MessageConsumer( String id) {

        this.kafkaConsumer =  new KafkaConsumer<String,String>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                        ConsumerConfig.GROUP_ID_CONFIG, id,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"
                )
        );

    }
}