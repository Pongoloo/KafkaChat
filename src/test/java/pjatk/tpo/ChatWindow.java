package pjatk.tpo;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatWindow extends JFrame{
    private JPanel mainPanel;
    private JTextArea chatView;
    private JTextField messageField;
    private JButton sendButton;
    private MessageConsumer messageConsumer;
    private final String consumerID;
    public ChatWindow(String id,int position) throws HeadlessException {
        this.consumerID=id;
        this.setTitle(id);
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
        messageConsumer.kafkaConsumer.subscribe(Collections.singletonList("haha"));
        executorService.submit(() -> {
            while (true) {
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> {
                    chatView.append(m.value() + '\n');
                });
            }
        });
    }
}
