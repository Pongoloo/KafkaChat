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

    boolean firstTimeReading = true;
    private Map<String,Set<String>> mutedUsers = new HashMap<>();
    private static Map<String,Set<String>> bannedUsers = new HashMap<>();
    private static Map<String,String> chatAdmins = new HashMap<>();

    public ChatWindow(String id, int position) throws HeadlessException {

        availableChatsModel = new DefaultComboBoxModel<>();
        availableUsersModel = new DefaultComboBoxModel<>();
        availableChats.setModel(availableChatsModel);
        availableUsers.setModel(availableUsersModel);

        this.consumerID = id;
        this.setTitle(id + " chat");
        this.add(mainPanel);
        this.setPreferredSize(new Dimension(500, 400));
        this.setLocation(position, 600);
        this.pack();
        this.setVisible(true);
        availableChatsModel.addAll(LoginWindow.userChats.get(consumerID));
        messageConsumer = new MessageConsumer(id);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        startReading(executorService);
        addListeners();
    }

    private void handleMessage(ConsumerRecord<String, String> message) {
        System.out.println("reading:" + consumerID + " topic:" + message.topic() + " message:" + message.value());
        if (message.topic().equals(metaDataTopic)) {
            //LOGOUT
            if (message.value().startsWith("logout")) {
                if (message.value().equals("logout " + consumerID)) {
                    handleOffsets();
                    messageConsumer.kafkaConsumer.close();
                    dispose();
                } else {
                    String loggedOutUser = message.value().substring(7);
                    availableUsersModel.removeElement(loggedOutUser);
                }
            }

            //LOGIN
            else if (message.value().startsWith("login")) {
                String newUser = message.value().substring(6);
                availableUsersModel.addElement(newUser);
            }
            // SWITCH CHAT
            else if (message.value().startsWith("switch")) {
                handleSwitchMessage(message);
            }
            // CREATE CHAT
            else if (message.value().startsWith("create")) {

                String[] split = message.value().split(" ");
                String chatName = split[1];
                String creator= split[2];
                String users = split[3];
                String userList = users.substring(7);
                StringBuilder sbd = new StringBuilder();
                Set<String> usersInvited = new HashSet<>();
                for (int i = 0; i < userList.length()-1; i++) {
                    if(userList.charAt(i)==','){
                        usersInvited.add(sbd.toString());
                        sbd=new StringBuilder();
                    } else{
                        sbd.append(userList.charAt(i));
                    }
                }
                usersInvited.add(sbd.toString());
                System.out.println("KURWA KURWA KURWA KURWA ");
                System.out.println(usersInvited);
                for (String user : usersInvited) {
                    LoginWindow.userChats.get(user).add(chatName);
                }
                if (usersInvited.contains(consumerID)) {
                    availableChatsModel.addElement(chatName);
                    chatAdmins.put(chatName,creator);
                    System.out.println(chatAdmins);
                }

            }
            // GET AVAILABLE USERS AND CHATS
            else if (message.value().startsWith("users:") && justLoggedIn) {
                String[] usersAndChats = message.value().split("]chats:\\[");
                String users = usersAndChats[0].substring(7);
                StringBuilder sbd = new StringBuilder();
                addAvailableUsers(users, sbd);
                addAvailableChats();
                justLoggedIn = false;
            }
            else if(message.value().startsWith("kick")){
                String userAndTopic = message.value().substring(5);
                StringBuilder sbd = new StringBuilder();
                String user="";
                String topic="";
                for (int i = 0; i < userAndTopic.length(); i++) {
                    if(userAndTopic.charAt(i)==' '){
                        user=sbd.toString();
                        sbd=new StringBuilder();
                    } else{
                        sbd.append(userAndTopic.charAt(i));
                    }
                }
                topic=sbd.toString();
                if(consumerID.equals(user)){
                    handleOffsets();
                    messageConsumer.kafkaConsumer.close();
                    messageConsumer=new MessageConsumer(consumerID);
                    availableChatsModel.removeElement(topic);
                    chatView.append("You've been kicked from the chat :C\n");
                    currentTopic="";
                    messageConsumer.kafkaConsumer.subscribe(Collections.singletonList(metaDataTopic));
                }
            }
            else if(message.value().startsWith("invite ")){
                String userAndChat = message.value().substring(7);
                StringBuilder sbd = new StringBuilder();
                String user="";
                String chat="";
                for (int i = 0; i < userAndChat.length(); i++) {
                    if(userAndChat.charAt(i)==' '){
                        user=sbd.toString();
                        sbd=new StringBuilder();
                    }else{
                        sbd.append(userAndChat.charAt(i));
                    }
                }
                chat=sbd.toString();
                if(consumerID.equals(user)){
                    availableChatsModel.addElement(chat);
                }
            }
            else if(message.value().startsWith("ban")){
                String[] split = message.value().split(" ");
                String userToBan = split[1];
                String fromTopic = split[2];
                if(consumerID.equals(userToBan)){
                    handleOffsets();
                    messageConsumer.kafkaConsumer.close();
                    messageConsumer=new MessageConsumer(consumerID);
                    availableChatsModel.removeElement(fromTopic);
                    chatView.append("You've been banned from the chat :C\n");
                    bannedUsers.putIfAbsent(currentTopic,new HashSet<>());
                    bannedUsers.get(currentTopic).add(consumerID);
                    currentTopic="";
                    messageConsumer.kafkaConsumer.subscribe(Collections.singletonList(metaDataTopic));
                }
            }
        } else {
            if (MessageConsumer.userOffsets.get(consumerID).get(currentTopic) == null) {
                System.out.println(messageConsumer.kafkaConsumer.assignment().size() + " SIZE ASSIGNMENTOW");
                Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
                Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
                long partitionOffset = 0;
                for (TopicPartition key : topicPartitionLongMap.keySet()) {
                    if (key.topic().equals(currentTopic)) {
                        partitionOffset = topicPartitionLongMap.get(key) - 1;
                        System.out.println(partitionOffset + " PRZYPISANY OFFSET DLA " + consumerID + " na " + currentTopic);
                    }
                }
                MessageConsumer.userOffsets.get(consumerID).put(currentTopic, partitionOffset);
            }
            Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
            if(!checkIfMuted(message)){
                chatView.append(message.value() + '\n');
            }
        }
    }
    private boolean checkIfMuted(ConsumerRecord<String,String> message){
        //11:11:11 xd-mesag
        String userAndMessage = message.value().substring(9);
        StringBuilder sbd = new StringBuilder();
        String userToMute="";
        for (int i = 0; i <userAndMessage.length(); i++) {
            if(userAndMessage.charAt(i)=='-'){
                userToMute=sbd.toString();
                break;
            }
            else{
                sbd.append(userAndMessage.charAt(i));
            }
        }
        if(mutedUsers.get(message.topic())!=null){
            if(mutedUsers.get(message.topic()).contains(userToMute)){
                return true;
            }
        }
        return false;
    }
    private void handleSendingMessage(){
        String message = messageField.getText();
        if(message.equals("/help")){
            JOptionPane.showMessageDialog(this, """
                        /mute {Username} mutes a specific user in this chat \

                        /kick {Username} kicks a person from the current chat \
                        
                        /ban {Username} only accessible to the creator of the chat, kicks someone without possibility to come back unless unbanned\
                        
                        /unban {Username} only accessible to the creator of the chat, lifts the ban and the person can be invited to the chat again\
                        
                        /invite {Username} adds a person to the currently open chat""");
        }
        else if(message.startsWith("/kick ")){
            String userToKick = message.substring(6);
            LoginWindow.userChats.get(userToKick).remove(currentTopic);
            MessageProducer.send(new ProducerRecord<>(metaDataTopic, "kick "+userToKick + " "+currentTopic));
            messageField.setText("");
        }
        else if(message.startsWith("/invite ")){
            String userToInvite = message.substring(8);
            LoginWindow.userChats.get(userToInvite).add(currentTopic);
            if(bannedUsers.get(currentTopic).contains(userToInvite)){
                JOptionPane.showMessageDialog(this,"this user is banned from this chat");
            } else{
                MessageProducer.send(new ProducerRecord<>(metaDataTopic,"invite "+userToInvite + " " + currentTopic));
                messageField.setText("");
            }
        }
        else if (message.startsWith("/mute ")){
            String userToMute = message.substring(6);
            mutedUsers.putIfAbsent(currentTopic,new HashSet<>());
            mutedUsers.get(currentTopic).add(userToMute);
            messageField.setText("");
        }
        else if (message.startsWith("/ban ")){
            if(chatAdmins.get(currentTopic).equals(consumerID)){
                String userToBan = message.substring(5);
                LoginWindow.userChats.get(userToBan).remove(currentTopic);
                MessageProducer.send(new ProducerRecord<>(metaDataTopic,"ban "+userToBan+" " + currentTopic));
                messageField.setText("");
            } else{
                JOptionPane.showMessageDialog(this,"You're not the creator of the chat, that function is forbidden.");
            }
        }
        else if(message.startsWith("/unban ")){
            if(chatAdmins.get(currentTopic).equals(consumerID)){
                String userToUnban = message.substring(7);
                bannedUsers.get(currentTopic).remove(userToUnban);
                messageField.setText("");
            } else{
                JOptionPane.showMessageDialog(this,"You're not the creator of the chat, that function is forbidden.");

            }
        }
        else{
            MessageProducer.send(new ProducerRecord<>(currentTopic, formatMessage(messageField.getText())));
            messageField.setText("");
        }
    }
    private void addListeners() {
        this.messageField.addActionListener(e -> {
            handleSendingMessage();
        });
        this.chatNameField.addActionListener(e -> {
            handleSendingMessage();
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

    private void handleCreatingChat() {
        if (chatNameField.getText().contains(":") || chatNameField.getText().contains(" ")) {
            JOptionPane.showMessageDialog(this, "Chat name can not contain {',' , ':' , ' '}");
        } else {
            Set<String> usersModel = new HashSet<>();
            for (int i = 0; i < availableUsersModel.getSize(); i++) {
                usersModel.add(availableUsersModel.getElementAt(i));
            }
            Rectangle bounds = this.getBounds();
            double x = bounds.getX();
            double y = bounds.getY();
            new ChatCreator(usersModel, chatNameField.getText(), x, y,consumerID);
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


    private boolean gotData = false;

    private void startReading(ExecutorService executorService) {
        ArrayList<String> topics = new ArrayList<>();
        topics.add(currentTopic);
        topics.add(metaDataTopic);
        messageConsumer.kafkaConsumer.subscribe(topics);
        executorService.submit(() -> {
            while (true) {
                ConsumerRecords<String, String> records = messageConsumer.kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    handleMessage(record);
                }
                if (!gotData) {
                    Set<TopicPartition> assignment = messageConsumer.kafkaConsumer.assignment();
                    if (assignment.size()>1) {
                        MessageProducer.send(new ProducerRecord<>(metaDataTopic, "metadata", "fetch users&chats "+consumerID));
                        gotData=true;
                    }
                }

            }
        });
    }

    private void addAvailableUsers(String users, StringBuilder sbd) {
        availableUsers.removeAllItems();
        for (int i = 0; i < users.length(); i++) {
            if (users.charAt(i) == ',') {
                availableUsersModel.addElement(sbd.toString());
                sbd = new StringBuilder();
                i++;
            } else {
                sbd.append(users.charAt(i));
            }
        }
        availableUsersModel.addElement(sbd.toString());
    }

    private void addAvailableChats() {
        availableChats.removeAllItems();
        availableChatsModel.addAll(LoginWindow.userChats.get(consumerID));
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
                if (offset == null) {
                    return;
                }
                messageConsumer.kafkaConsumer.seek(topicPartition, offset);
                Map<TopicPartition, Long> topicPartitionLongMap = messageConsumer.kafkaConsumer.endOffsets(assignment);
                System.out.println(topicPartitionLongMap);
                Map<TopicPartition, Long> topicPartitionLongMap1 = messageConsumer.kafkaConsumer.beginningOffsets(assignment);
                System.out.println(topicPartitionLongMap1);
            }
        }
    }
}
