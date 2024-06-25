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
import java.util.Set;

public class MessageConsumer{
    KafkaConsumer<String,String> kafkaConsumer;

    // user -> map<topic,offset>
    static Map<String,Map<String, Long>> userOffsets = new HashMap<>();
    static Map<String, Set<String>> userTopics = new HashMap<>();

    public MessageConsumer( String id) {
        if (userOffsets.get(id)==null) {
            userOffsets.put(id,new HashMap<>());
            userOffsets.get(id).put("metadata", 0L);
        }

        this.kafkaConsumer =  new KafkaConsumer<String,String>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                        ConsumerConfig.GROUP_ID_CONFIG, id,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
                )
        );

    }
}