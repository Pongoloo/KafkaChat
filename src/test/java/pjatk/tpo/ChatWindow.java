package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.swing.*;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ChatWindow extends JFrame {
    private JPanel mainPanel;
    private JButton sendButton;
    private JTextArea messageField;
    private JButton loginButton;
    private JTextField userID;
    private JComboBox availableChats;
    private JTextArea chatView;
    private JButton chatSwitchButton;
    private JButton addChatButton;
    private JTextField newChatNameField;
    private JComboBox activeUsers;
    private MessageConsumer messageConsumer;
    // ToDo trzeba bedzie zmienic tutaj zeby ta mapa miala <topic=string, Map<TopicPartition,Timestamp>> i pozniej tymi offsetami bedzie mozna odczytywac
    private Map<String, List<String>> chats = new HashMap<>();
    private final String defaultChatName="chat";
    private boolean isLoggedIn=false;
    private Set<String> allUsers= new HashSet<>();

    private void login(){
        String userIDText = this.userID.getText();
        if(allUsers.add(userIDText)){
            messageConsumer=new MessageConsumer(userIDText);
            enableChat(userIDText);
        }else{
            messageConsumer.kafkaConsumer.subscribe(Collections.singleton("chat"));
            messageConsumer.kafkaConsumer.offsetsForTimes(messageConsumer.userOffsets);
        }
        setTitle(userIDText  + " CHAT");
        this.userID.setText("");
        chats.putIfAbsent(defaultChatName, new ArrayList<>());
        isLoggedIn=true;
        loginButton.setText("logout");
        this.revalidate();
        this.repaint();

        List<PartitionInfo> chat = messageConsumer.kafkaConsumer.partitionsFor("chat");
        List<TopicPartition> partitions = chat.stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
        messageConsumer.userOffsets=
                Map.of(partitions.get(0),Timestamp.from(Instant.now()).getTime()
        );
    }
    private void logout(){
        this.setTitle("Chat");
        isLoggedIn=false;
        loginButton.setText("login");
        this.revalidate();
        this.repaint();
        messageConsumer.kafkaConsumer.unsubscribe();
    }
    private void addLoginAndLogout(){
        loginButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(isLoggedIn){
                    logout();
                }else{
                    login();
                }
            }
        });
    }
    private void enableChat(String id){
        messageConsumer.kafkaConsumer.subscribe(Collections.singletonList(defaultChatName));
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        startReading(executorService);
        addSendingMessages(id);
        addAddingChats();
        addSwitchingChats();
    }
    public ChatWindow(String topic, String id) throws InterruptedException {
        setUpFrame(topic, id);
        changeGraphics();
        addLoginAndLogout();
    }
    private void setUpFrame(String topic, String id) {
     //   messageConsumer = new MessageConsumer(id);
        this.availableChats.addItem(defaultChatName);
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);
        this.setPreferredSize(new Dimension(600, 800));
        this.add(mainPanel);
        this.setVisible(true);
        this.setTitle(id + " CHAT");
        this.pack();
    }

    private void startReading(ExecutorService executorService) {
        executorService.submit(() -> {
            while (true) {
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> {
                    processMessage(m);
                    //   chatView.append(m.value() + '\n');
                });
            }
        });
    }
    private String formatTime(int time){
        if(time<10){
            return "0"+time;
        } else{
            return time+"";
        }
    }
    private void addSendingMessages(String id) {
        this.sendButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                LocalDateTime now = LocalDateTime.now();
                String timestamp = formatTime(now.getHour()) + ":" + formatTime(now.getMinute()) + ":" + formatTime(now.getSecond());

                MessageProducer.send(new ProducerRecord<>(availableChats.getSelectedItem().toString(), timestamp + " - " + id + ":" + messageField.getText().strip()));
                messageField.setText("");
            }
        });
    }

    private void addAddingChats() {
        addChatButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                //  availableChats.addItem(newChatNameField.getText());
                // now we want to send a special message to everyone so everyone knows about the new chat
                MessageProducer.send(new ProducerRecord<>("chat", "SYSTEM.INFO Created chat:" + newChatNameField.getText()));
                newChatNameField.setText("");
            }
        });
    }

    private void addSwitchingChats() {
        chatSwitchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String chatText = chats.get(availableChats.getSelectedItem()).stream().collect(Collectors.joining());
                chatView.setText(chatText);
//                messageConsumer.kafkaConsumer.unsubscribe();
//                messageConsumer.kafkaConsumer.ad(Collections.singleton(availableChats.getSelectedItem().toString()));
            }
        });
    }



    private void changeGraphics() {
        chatView.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 20));
        this.messageField.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 20));
    }

    private void processMessage(ConsumerRecord<String, String> message) {
        if (message.value().startsWith("SYSTEM.INFO Created chat")) {
            int chatNameBeginIndex = message.value().indexOf(":");
            String topic = message.value().substring(chatNameBeginIndex + 1);
            availableChats.addItem(topic);
            chats.put(topic, new ArrayList<>());
            // testing subbing to many :D

            messageConsumer.kafkaConsumer.subscribe(chats.keySet());
        } else {
            chats.get(message.topic()).add(message.value() + '\n');
            if (message.topic().equals(availableChats.getSelectedItem())) {
                chatView.append(message.value() + '\n');
            }

        }
    }
}

