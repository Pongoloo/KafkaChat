package pjatk.tpo;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
    private final MessageConsumer messageConsumer;

    public ChatWindow(String topic, String id) {
        messageConsumer = new MessageConsumer(topic, id);
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);
        this.setPreferredSize(new Dimension(600, 800));
        this.add(mainPanel);
        this.setVisible(true);
        this.setTitle("WOop");
        this.pack();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(() -> {
            while(true){
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> {
                    System.out.println(m.value());
                    processMessage(m.value());
                 //   chatView.append(m.value() + '\n');
                 //   System.out.println("KURWA KURWA KURWA KURWA KURWA KURWA");
                });
            }

        });

        sendButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                MessageProducer.send(new ProducerRecord<>("chat", id + " " + messageField.getText().strip()));
            }
        });

    }
    private void processMessage(String message){
        if(message.startsWith("SYSTEM.INFO Created chat")){
            int chatNameBeginIndex = message.indexOf(":");
            availableChats.addItem(message.substring(chatNameBeginIndex+1));
        } else{
            chatView.append(message +'\n');
        }
    }
}

