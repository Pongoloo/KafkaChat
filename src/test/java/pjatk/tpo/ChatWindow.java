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
import java.util.Map;
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
    private JComboBox availableUsers;
    private MessageConsumer messageConsumer;
    private final String consumerID;
    private final String metaDataTopic="metadata";
    private String currentTopic="haha";
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
            MessageProducer.send(new ProducerRecord<>(currentTopic,formatMessage(messageField.getText())));
            messageField.setText("");
        });
        logoutButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic,"logout "+ id));
        });
        goToButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic,"switch to:"+availableChats.getSelectedItem()+"}user="+id));
        });
        createButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic,"create "+chatNameField.getText()));
            chatNameField.setText("");
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
                Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
                for (TopicPartition topicPartition : assignment) {
                    if(!topicPartition.topic().equals("metadata")){
                        messageConsumer.kafkaConsumer.seek(topicPartition,0);
                        Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
                        System.out.println(topicPartitionLongMap);
                        Map<TopicPartition, Long> topicPartitionLongMap1 = messageConsumer.kafkaConsumer.beginningOffsets(assignment);
                        System.out.println(topicPartitionLongMap1);
                    }
                }
                System.out.println("DEBUG BITCH");
                System.out.println(messageConsumer.kafkaConsumer.endOffsets(assignment));
                messageConsumer.kafkaConsumer.close();
                dispose();
            } else if(message.value().startsWith("login")){
                String newUser = message.value().substring(6);
                availableUsers.addItem(newUser);
            }else if(message.value().startsWith("switch")){
                String chatAndUser = message.value().substring(10);
                String[] split = chatAndUser.split("}");
                if(consumerID.equals(split[1].substring(5))){
                    ArrayList<String> topics = new ArrayList<>();
                    topics.add(metaDataTopic);
                    topics.add(split[0]);
                    messageConsumer.kafkaConsumer.subscribe(topics);
                    currentTopic=split[0];
                    chatView.setText("");
                }
            } else if(message.value().startsWith("create")){
                availableChats.addItem(message.value().substring(7));
            }
        } else{
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            System.out.println(messageConsumer.kafkaConsumer.endOffsets(assignment));
            System.out.println(message.topic() + " " + message.value() + " " + consumerID);
            chatView.append(message.value() + '\n');
        }

    }
}
