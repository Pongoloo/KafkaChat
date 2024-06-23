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

public class ChatWindow extends JFrame {
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
    private final String metaDataTopic = "metadata";
    private String currentTopic = "default";
    private boolean justLoggedIn = true;

    public ChatWindow(String id, int position) throws HeadlessException {
        availableChats.addItem(currentTopic);
        this.consumerID = id;
        this.setTitle(id + " chat");
        this.add(mainPanel);
        this.setPreferredSize(new Dimension(500, 400));
        this.setLocation(position, 600);
        this.pack();
        this.setVisible(true);
        messageConsumer = new MessageConsumer(id);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        startReading(executorService);
        sendButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(currentTopic, formatMessage(messageField.getText())));
            messageField.setText("");
        });
        logoutButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic, "logout " + id));
        });
        goToButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic, "switch to:" + availableChats.getSelectedItem() + "}user=" + id));
        });
        createButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic, "create " + chatNameField.getText()));
            chatNameField.setText("");
        });
    }

    private String formatTime(int time) {
        if (time < 10) {
            return "0" + time;
        } else {
            return "" + time;
        }
    }

    private String formatMessage(String message) {
        LocalDateTime now = LocalDateTime.now();
        String hour = formatTime(now.getHour());
        String minute = formatTime(now.getMinute());
        String second = formatTime(now.getSecond());
        return hour + ":" + minute + ":" + second + " " + consumerID + "-" + message;

    }

    private void startReading(ExecutorService executorService) {

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(currentTopic);
        topics.add(metaDataTopic);
        messageConsumer.kafkaConsumer.subscribe(topics);
        if (MessageConsumer.userOffsets.get(consumerID).get(currentTopic) == null) {
            messageConsumer.kafkaConsumer.poll(Duration.of(50, ChronoUnit.MILLIS)).forEach(this::handleMessage);
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
            long partitionOffset = 0;
            for (TopicPartition key : topicPartitionLongMap.keySet()) {
                if (key.topic().equals(currentTopic)) {
                    partitionOffset = topicPartitionLongMap.get(key);
                }
            }
            MessageConsumer.userOffsets.get(consumerID).put(currentTopic, partitionOffset);
        }
        executorService.submit(() -> {
            while (true) {

                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(this::handleMessage);
            }
        });
    }

    private void handleMessage(ConsumerRecord<String, String> message) {
        System.out.println("AT THE BEGINNNIG ON THE FUNCTION messageTopic:" + message.topic() + " messageValue:" + message.value() + " currently reading dude:" + consumerID);

        if (message.topic().equals(metaDataTopic)) {
            //LOGOUT
            if (message.value().equals("logout " + consumerID)) {
                handleOffsets();
                messageConsumer.kafkaConsumer.close();
                dispose();
            }
            //LOGIN
            else if (message.value().startsWith("login")) {
                String newUser = message.value().substring(6);
                availableUsers.addItem(newUser);
            }
            // SWITCH CHAT
            else if (message.value().startsWith("switch")) {
                handleSwitchMessage(message);
            }
            // CREATE CHAT
            else if (message.value().startsWith("create")) {
                availableChats.addItem(message.value().substring(7));
            }
            // GET AVAILABLE USERS AND CHATS
            else if (message.value().startsWith("users:") && justLoggedIn) {
                String[] usersAndChats = message.value().split("]chats:\\[");
                String users = usersAndChats[0].substring(7);
                StringBuilder sbd = new StringBuilder();
                addAvailableUsers(users, sbd);
                sbd = new StringBuilder();
                addAvailableChats(usersAndChats[1], sbd);
                justLoggedIn = false;
            }
        } else {
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            System.out.println(messageConsumer.kafkaConsumer.endOffsets(assignment) + " offsets?");
            System.out.println("topic +  value + consumerID");
            System.out.println(message.topic() + " " + message.value() + " " + consumerID);
            chatView.append(message.value() + '\n');
        }

    }

    private void addAvailableUsers(String users, StringBuilder sbd) {
        availableUsers.removeAllItems();
        for (int i = 0; i < users.length(); i++) {
            if (users.charAt(i) == ',') {
                availableUsers.addItem(sbd.toString());
                sbd = new StringBuilder();
            } else {
                sbd.append(users.charAt(i));
            }
        }
        availableUsers.addItem(sbd.toString());
    }

    private void addAvailableChats(String chats, StringBuilder sbd) {
        availableChats.removeAllItems();
        for (int i = 0; i < chats.length() - 1; i++) {
            if (chats.charAt(i) == ',') {
                availableChats.addItem(sbd.toString());
                sbd = new StringBuilder();
            } else {
                sbd.append(chats.charAt(i));
            }
        }
        availableChats.addItem(sbd.toString());
    }

    private void handleSwitchMessage(ConsumerRecord<String, String> message) {
        String user = getUserFromSwitchMessage(message);
        String topic = getTopicFromSwitchMessage(message);
        if (consumerID.equals(user)) {
            handleOffsets();
            subscribeToNewTopics(topic);
            messageConsumer.kafkaConsumer.poll(Duration.of(50, ChronoUnit.MILLIS)).forEach(m ->{
                System.out.println(m.value() + " FROM A PLACE WHERE IT SHOULDNT BER");
            });
            currentTopic = topic;
            putNewOffsetIfAbsent();
            chatView.setText("");
        }
    }

    private void subscribeToNewTopics(String newTopic) {
        ArrayList<String> topics = new ArrayList<>();
        topics.add(metaDataTopic);
        topics.add(newTopic);
        messageConsumer.kafkaConsumer.subscribe(topics);
    }

    private String getUserFromSwitchMessage(ConsumerRecord<String, String> message) {
        String chatAndUser = message.value().substring(10);
        String[] split = chatAndUser.split("}");
        return split[1].substring(5);
    }

    private String getTopicFromSwitchMessage(ConsumerRecord<String, String> message) {
        String chatAndUser = message.value().substring(10);
        String[] split = chatAndUser.split("}");
        return split[0];
    }

    private void putNewOffsetIfAbsent() {
        if (MessageConsumer.userOffsets.get(consumerID).get(currentTopic) == null) {
            messageConsumer.kafkaConsumer.poll(Duration.of(50, ChronoUnit.MILLIS)).forEach(m ->
            {
                System.out.println(m.value()+ " FROM THE PLACE WHERE IT SHOULDNT BE");
            });
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
            long partitionOffset = 0;
            for (TopicPartition key : topicPartitionLongMap.keySet()) {
                if (key.topic().equals(currentTopic)) {
                    partitionOffset = topicPartitionLongMap.get(key);
                }
            }
            MessageConsumer.userOffsets.get(consumerID).put(currentTopic, partitionOffset);
        }
    }

    private void handleOffsets() {
        Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
        for (TopicPartition topicPartition : assignment) {
            if (!topicPartition.topic().equals("metadata")) {
                Long offset = MessageConsumer.userOffsets.get(consumerID).get(topicPartition.topic());
                System.out.println(offset + "XD");
                messageConsumer.kafkaConsumer.seek(topicPartition, offset);
                Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
                System.out.println(topicPartitionLongMap);
                Map<TopicPartition, Long> topicPartitionLongMap1 = messageConsumer.kafkaConsumer.beginningOffsets(assignment);
                System.out.println(topicPartitionLongMap1);
            }
        }
    }
}
