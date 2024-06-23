package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatWindow extends JFrame {
    private JPanel mainPanel;
    private JTextArea chatView;
    private JTextField messageField;
    private JButton sendButton;
    private JTextField chatNameField;
    private JButton createButton;
    private JButton goToButton;
    private JButton logoutButton;
    private MessageConsumer messageConsumer;
    private final String consumerID;
    private final String metaDataTopic = "metadata";
    private String currentTopic = "default";
    private boolean justLoggedIn = true;

    private JComboBox availableUsers;
    private DefaultComboBoxModel<String> availableUsersModel;
    private JComboBox availableChats;
    private DefaultComboBoxModel<String> availableChatsModel;


    public ChatWindow(String id, int position) throws HeadlessException {
        availableUsersModel = new DefaultComboBoxModel<>();
        availableUsers.setModel(availableUsersModel);
        availableChatsModel = new DefaultComboBoxModel<>();
        availableChats.setModel(availableChatsModel);

        availableChatsModel.addElement(currentTopic);
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
        addListeners();
    }
    private void addListeners(){
        this.messageField.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(currentTopic, formatMessage(messageField.getText())));
            messageField.setText("");
        });
        this.chatNameField.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic, "create " + chatNameField.getText()));
            chatNameField.setText("");
        });
        this.addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                MessageProducer.send(new ProducerRecord<>(metaDataTopic, "logout " + consumerID));
            }
        });
        sendButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(currentTopic, formatMessage(messageField.getText())));
            messageField.setText("");
        });
        logoutButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic, "logout " + consumerID));
        });
        goToButton.addActionListener(e -> {
            MessageProducer.send(new ProducerRecord<>(metaDataTopic, "switch to:" + availableChats.getSelectedItem() + "}user=" + consumerID));
        });
        createButton.addActionListener(e -> {
            handleCreatingChat();
        });
    }
    private void handleCreatingChat(){
        if(chatNameField.getText().contains(":") || chatNameField.getText().contains(" ")){
            JOptionPane.showMessageDialog(this, "Chat name can not contain {',' , ':' , ' '}");
        } else{
            Set<String> usersModel = new HashSet<>();
            for (int i = 0; i < availableUsersModel.getSize(); i++) {
                usersModel.add(availableUsersModel.getElementAt(i));
            }
            Rectangle bounds = this.getBounds();
            double x = bounds.getX();
            double y = bounds.getY();
            new ChatCreator(usersModel,chatNameField.getText(),x,y);
            chatNameField.setText("");
        }
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
        executorService.submit(() -> {
            while (true) {
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(this::handleMessage);
            }
        });
    }

    private void handleMessage(ConsumerRecord<String, String> message) {

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
                String chatNameAndUsers = message.value().substring(7);
                int i = chatNameAndUsers.indexOf(' ');
                String chatName = chatNameAndUsers.substring(0, i);
                int userListIndex = chatNameAndUsers.indexOf(':');
                String userListString = chatNameAndUsers.substring(userListIndex + 2,chatNameAndUsers.length()-1);
                StringBuilder sbd = new StringBuilder();
                Set<String> userInvited = new HashSet<>();
                for (int j = 0; j < userListString.length(); j++) {
                    if(userListString.charAt(j)==','){
                        userInvited.add(sbd.toString());
                        sbd= new StringBuilder();
                    } else{
                        sbd.append(userListString.charAt(j));
                    }
                }
                userInvited.add(sbd.toString());
                if(userInvited.contains(consumerID)){
                    availableChats.addItem(chatName);
                }
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
            if (MessageConsumer.userOffsets.get(consumerID).get(currentTopic) == null) {
                System.out.println(messageConsumer.kafkaConsumer.assignment().size() + " SIZE ASSIGNMENTOW");
                Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
                Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
                long partitionOffset = 0;
                for (TopicPartition key : topicPartitionLongMap.keySet()) {
                    if (key.topic().equals(currentTopic)) {
                        partitionOffset = topicPartitionLongMap.get(key)-1;
                        System.out.println(partitionOffset + " PRZYPISANY OFFSET DLA " + consumerID + " na " +currentTopic );
                    }
                }
                MessageConsumer.userOffsets.get(consumerID).put(currentTopic, partitionOffset);
            }

            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            chatView.append(message.value() + '\n');
        }
    }

    private void addAvailableUsers(String users, StringBuilder sbd) {
        availableUsers.removeAllItems();
        for (int i = 0; i < users.length(); i++) {
            if (users.charAt(i) == ',') {
                availableUsers.addItem(sbd.toString());
                sbd = new StringBuilder();
                i++;
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
                i++;
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
            messageConsumer.kafkaConsumer.poll(Duration.ofMillis(0));
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
            messageConsumer.kafkaConsumer.poll(Duration.of(0, ChronoUnit.MILLIS));
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
