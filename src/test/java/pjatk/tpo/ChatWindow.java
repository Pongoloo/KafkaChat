package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatWindow extends JFrame{
    private JPanel mainPanel;
    private JTextArea chatView;
    private JTextField messageField;
    private JButton sendButton;
    private JTextField chatNameField;
    private JButton createButton;
    private JComboBox availableChats;
    private JButton goToButton;
    private JButton logoutButton;
    private MessageConsumer messageConsumer;
    private final String consumerID;
    private final String metaDataTopic="metadata";
    public ChatWindow(String id,int position) throws HeadlessException {
        this.consumerID=id;
        this.setTitle(id + " chat");
        this.add(mainPanel);
        this.setPreferredSize(new Dimension(500,400));
        this.setLocation(position,600);
        this.pack();
        this.setVisible(true);
        messageConsumer = new MessageConsumer(id);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        startReading(executorService);
        sendButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>("haha",formatMessage(messageField.getText())));
            messageField.setText("");
        });
        logoutButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic,"logout "+ id));
        });
    }
    private String formatTime(int time){
        if(time<10){
            return "0"+time;
        } else{
            return ""+time;
        }
    }
    private String formatMessage(String message){
        LocalDateTime now = LocalDateTime.now();
        String hour = formatTime(now.getHour());
        String minute = formatTime(now.getMinute());
        String second = formatTime(now.getSecond());
        return hour+":"+minute+":"+second+" "+consumerID+"-"+message;

    }
    private void startReading(ExecutorService executorService) {

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("haha");
        topics.add(metaDataTopic);
        messageConsumer.kafkaConsumer.subscribe(topics);
        Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
        messageConsumer.kafkaConsumer.seekToBeginning(assignment);
        executorService.submit(() -> {
            while (true) {
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(this::handleMessage);
            }
        });
    }
    private void handleMessage(ConsumerRecord<String,String> message){
        if(message.topic().equals(metaDataTopic)){
            if(message.value().equals("logout "+consumerID)){
                // potencjalnei tu moze byc unsubcribew
                messageConsumer.kafkaConsumer.close();
                dispose();
            }
        } else{
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            System.out.println(assignment);
            System.out.println(message.topic() + " " + message.value() + " " + consumerID);
            chatView.append(message.value() + '\n');
        }

    }
}
