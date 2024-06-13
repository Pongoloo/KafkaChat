package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

public class MessageConsumer{
    KafkaConsumer<String,String> kafkaConsumer;

    public MessageConsumer(String topic, String id) {

        this.kafkaConsumer =  new KafkaConsumer<String,String>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                        ConsumerConfig.GROUP_ID_CONFIG, id,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"
                )
        );
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> System.out.println(m.value()));
    }
}