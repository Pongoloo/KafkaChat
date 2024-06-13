package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.time.Duration;
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
    private JTextField textField1;
    private JComboBox availableChats;
    private JTextArea chatView;
    private JButton chatSwitchButton;
    private JButton addChatButton;
    private JTextField newChatNameField;
    private JComboBox activeUsers;
    private MessageConsumer messageConsumer;
    private Map<String, List<String>> chats = new HashMap<>();
    public ChatWindow(String topic, String id) {
        setUpFrame(topic, id);
        changeGraphics();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        startReading(executorService);
        addSendingMessages(id);
        addAddingChats();
        addSwitchingChats();
        loginButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

            }
        });
    }
    private void setUpFrame(String topic, String id) {
        messageConsumer = new MessageConsumer(topic, id);
        chats.put(topic, new ArrayList<>());
        this.availableChats.addItem(topic);
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
                    System.out.println("on chat:" + m.topic() + " mess:" + m.value());
                    processMessage(m);
                    //   chatView.append(m.value() + '\n');
                    //   System.out.println("KURWA KURWA KURWA KURWA KURWA KURWA");
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
        System.out.println("we are adding the actionlistener XD");
        this.sendButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.out.println("nigga wtf");
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
//                System.out.println("unsubscribed");
//                messageConsumer.kafkaConsumer.ad(Collections.singleton(availableChats.getSelectedItem().toString()));
//                System.out.println("subscriped to a new topic:"+availableChats.getSelectedItem().toString());
//              //  System.out.println("I WANNNNNNNNNNNNA SEE THIS ");
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

