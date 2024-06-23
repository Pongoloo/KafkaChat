package pjatk.tpo;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoginWindow extends JFrame {
    private JTextField loginField;
    private JButton loginButton;
    private JPanel mainPanel;

    private Set<String> currentlyLoggedUsers = new HashSet<>();
    private Set<String> currentlyActiveChats = new HashSet<>();

    private int amountOfChatWindows =0;
    private int screenWidth;
    private int screenHeight;
    private final MessageConsumer messageConsumer;
    private int getChatWindowPosition(){
        if(100+500*(amountOfChatWindows+1) > screenWidth){
            amountOfChatWindows=0;
        }
        return 100+500*amountOfChatWindows;
    }
    public LoginWindow() throws HeadlessException {
        messageConsumer=new MessageConsumer("coordinator");
        currentlyActiveChats.add("default");
        this.setPreferredSize(new Dimension(300,100));
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
         screenWidth = screenSize.width;
         screenHeight = screenSize.height;
        int x = (screenWidth - 300) / 2;
        int y = 300;

        // Set the frame location
        this.setLocation(x, y);

        System.out.println(screenHeight + "  "  + y);
        this.setTitle("Login page");
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.add(mainPanel);
        this.pack();
        this.setVisible(true);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        messageConsumer.kafkaConsumer.subscribe(Collections.singletonList("metadata"));
        executorService.submit(() -> {
            while (true) {
                messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(
                        m -> {
                            if(m.value().startsWith("logout")){
                                String user = m.value().substring(7);
                                currentlyLoggedUsers.remove(user);
                            } else if(m.value().startsWith("create")){
                                String chat = m.value().substring(7);
                                currentlyActiveChats.add(chat);
                                System.out.println("FROM COORDINATOR RECEIVED INFO ABOUT NEW CHAT:"+chat);
                            } else if(m.value().startsWith("login")){
                                String user = m.value().substring(6);
                                System.out.println(this.currentlyLoggedUsers);
                                System.out.println(this.currentlyActiveChats);
                                System.out.println("users:"+currentlyLoggedUsers + "chats:"+currentlyActiveChats);
                                MessageProducer.send(new ProducerRecord<>("metadata","users:"+currentlyLoggedUsers + "chats:"+currentlyActiveChats));
                            }
                        });
            }
        });
        loginField.addActionListener(e -> {
            login();
        });
        loginButton.addActionListener(e -> {
           login();
        });
    }
    private void login(){
        if(loginField.getText().equals("") ||
                loginField.getText()==null){
            JOptionPane.showMessageDialog(this, "imo 30 punktow za to");
        } else{
            SwingUtilities.invokeLater( () ->{
                int chatWindowPosition = getChatWindowPosition();
                amountOfChatWindows++;
                if(currentlyLoggedUsers.contains(loginField.getText())){
                    JOptionPane.showMessageDialog(this, "user "+loginField.getText()+ " already logged in");
                } else{
                    new ChatWindow(loginField.getText(), chatWindowPosition);
                    currentlyLoggedUsers.add(loginField.getText());
                    MessageProducer.send(new ProducerRecord<>("metadata","login "+loginField.getText()));
                    loginField.setText("");
                }
            });
        }
    }
}
