package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import javax.swing.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) {
        EmbeddedKafkaBroker embeddedKafkaBroker = new EmbeddedKafkaBroker(1).
                kafkaPorts(9092);

        embeddedKafkaBroker.afterPropertiesSet();

        //toDo u wanna make sure that u have all the topics and u inform if smth new change
        List<ChatWindow> readers= new ArrayList<>();
        SwingUtilities.invokeLater( () ->{
            ChatWindow chatWindow = null;
            try {
                chatWindow = new ChatWindow("chat", "Kinga");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            readers.add(chatWindow);
                });
        SwingUtilities.invokeLater( () -> {
            ChatWindow chatWindow = null;
            try {
                chatWindow = new ChatWindow("chat", "Jak00b");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            readers.add(chatWindow);
        });



    }
}


