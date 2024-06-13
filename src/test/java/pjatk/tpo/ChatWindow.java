package pjatk.tpo;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private final MessageConsumer messageConsumer;
    private List<String> topics;

    public ChatWindow(String topic, String id) {
        messageConsumer = new MessageConsumer(topic, id);
        topics= new ArrayList<>();
        topics.add(topic);
        this.availableChats.addItem(topic);
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);
        this.setPreferredSize(new Dimension(600, 800));
        this.add(mainPanel);
        this.setVisible(true);
        this.setTitle("WOop");
        this.pack();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        // READER FUNCTION
        executorService.submit(() -> {
            while(true){
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> {
                    System.out.println("on chat:"+m.topic()+" mess:"+m.value());
                    processMessage(m.value());
                 //   chatView.append(m.value() + '\n');
                 //   System.out.println("KURWA KURWA KURWA KURWA KURWA KURWA");
                });
            }

        });

        // SEND MESSAGE FUNCTION
        sendButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                MessageProducer.send(new ProducerRecord<>(availableChats.getSelectedItem().toString(), id + " " + messageField.getText().strip()));
            }
        });



        //ADD CHAT FUNCTION
        addChatButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
              //  availableChats.addItem(newChatNameField.getText());
                // now we want to send a special message to everyone so everyone knows about the new chat
                MessageProducer.send(new ProducerRecord<>("chat","SYSTEM.INFO Created chat:"+newChatNameField.getText()));
                newChatNameField.setText("");
            }
        });
        //SWITCHING CHATS FUNCTION
        chatSwitchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

//                messageConsumer.kafkaConsumer.unsubscribe();
//                System.out.println("unsubscribed");
//                messageConsumer.kafkaConsumer.ad(Collections.singleton(availableChats.getSelectedItem().toString()));
//                System.out.println("subscriped to a new topic:"+availableChats.getSelectedItem().toString());
//              //  System.out.println("I WANNNNNNNNNNNNA SEE THIS ");
            }
        });
    }
    private void processMessage(String message){
        if(message.startsWith("SYSTEM.INFO Created chat")){
            int chatNameBeginIndex = message.indexOf(":");
            String topic = message.substring(chatNameBeginIndex + 1);
            availableChats.addItem(topic);
            topics.add(topic);
            // testing subbing to many :D

            messageConsumer.kafkaConsumer.subscribe(topics);
        } else{
            chatView.append(message +'\n');
        }
    }
}

