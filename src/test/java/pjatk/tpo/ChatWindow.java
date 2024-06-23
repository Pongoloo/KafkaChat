package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
    private String currentTopic="default";
    private boolean justLoggedIn=true;
    public ChatWindow(String id,int position) throws HeadlessException {
        availableChats.addItem(currentTopic);
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
        topics.add(currentTopic);
        topics.add(metaDataTopic);
        messageConsumer.kafkaConsumer.subscribe(topics);
        if (MessageConsumer.userOffsets.get(consumerID).get(currentTopic)==null) {
            messageConsumer.kafkaConsumer.poll(Duration.of(50, ChronoUnit.MILLIS)).forEach(this::handleMessage);
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
            long partitionOffset=0;
            for (TopicPartition key : topicPartitionLongMap.keySet()) {
                if (key.topic().equals(currentTopic)) {
                    partitionOffset=topicPartitionLongMap.get(key);
                }
            }
            MessageConsumer.userOffsets.get(consumerID).put(currentTopic,partitionOffset);
        }
        executorService.submit(() -> {
            while (true) {
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(this::handleMessage);
            }
        });
    }
    private void handleMessage(ConsumerRecord<String,String> message){
        System.out.println("AT THE BEGINNNIG ON THE FUNCTION messageTopic:" + message.topic() +  " messageValue:"+message.value() +  " currently reading dude:"+consumerID);

        if(message.topic().equals(metaDataTopic)){
            if(message.value().equals("logout "+consumerID)){
                // potencjalnei tu moze byc unsubcribew
                Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
                for (TopicPartition topicPartition : assignment) {
                    if(!topicPartition.topic().equals("metadata")){
                        Long offset = MessageConsumer.userOffsets.get(consumerID).get(topicPartition.topic());
                        System.out.println(offset + "XD");
                        messageConsumer.kafkaConsumer.seek(topicPartition,offset);
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
                    messageConsumer.kafkaConsumer.poll(Duration.of(50, ChronoUnit.MILLIS)).forEach(this::handleMessage);
                    currentTopic=split[0];
                    if (MessageConsumer.userOffsets.get(consumerID).get(currentTopic)==null) {
                        messageConsumer.kafkaConsumer.poll(Duration.of(50, ChronoUnit.MILLIS));
                        Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
                        Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
                        long partitionOffset=0;
                        for (TopicPartition key : topicPartitionLongMap.keySet()) {
                            if (key.topic().equals(currentTopic)) {
                                partitionOffset=topicPartitionLongMap.get(key);
                            }
                        }
                        MessageConsumer.userOffsets.get(consumerID).put(currentTopic,partitionOffset);
                    }
                    chatView.setText("");
                }
            } else if(message.value().startsWith("create")){
                availableChats.addItem(message.value().substring(7));
            } else if(message.value().startsWith("users:") && justLoggedIn){
                availableChats.removeAllItems();
                availableUsers.removeAllItems();
                System.out.println(message.value());
                String[] usersAndChats = message.value().split("]chats:\\[");
                System.out.println(usersAndChats[0]);
                System.out.println(usersAndChats[1]);
                String users = usersAndChats[0].substring(7);
                StringBuilder sbd = new StringBuilder();
                for (int i = 0; i < users.length(); i++) {
                    if(users.charAt(i)==','){
                        availableUsers.addItem(sbd.toString());
                        sbd= new StringBuilder();
                    } else{
                        sbd.append(users.charAt(i));
                    }
                }
                availableUsers.addItem(sbd.toString());
                sbd = new StringBuilder();
                for (int i = 0; i < usersAndChats[1].length()-1; i++) {
                    if(usersAndChats[1].charAt(i)==','){
                        availableChats.addItem(sbd.toString());
                        sbd= new StringBuilder();
                    } else{
                        sbd.append(usersAndChats[1].charAt(i));
                    }
                }
                availableChats.addItem(sbd.toString());
                justLoggedIn=false;
            }
        } else{
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            System.out.println(messageConsumer.kafkaConsumer.endOffsets(assignment) + " offsets?");
            System.out.println("topic +  value + consumerID");
            System.out.println(message.topic() + " " + message.value() + " " + consumerID);
            chatView.append(message.value() + '\n');
        }

    }
}
