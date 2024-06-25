package pjatk.tpo;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoginWindow extends JFrame {
    private JTextField loginField;
    private JButton loginButton;
    private JPanel mainPanel;
    private JPasswordField passwordField;
    private JButton signInButton;
    private JButton registerButton;
    private JButton forgotPasswordbutton;

    static private Set<UserCredentials> userCredentials = new HashSet<>();

    private Set<String> currentlyLoggedUsers = new HashSet<>();
    private Set<String> currentlyActiveChats = new HashSet<>();

    public static Map<String,Set<String>> userChats = new HashMap<>();
    private int amountOfChatWindows = 0;
    private int screenWidth;
    private int screenHeight;
    private final MessageConsumer messageConsumer;

    private int getChatWindowPosition() {
        if (100 + 500 * (amountOfChatWindows + 1) > screenWidth) {
            amountOfChatWindows = 0;
        }
        return 100 + 500 * amountOfChatWindows;
    }

    public LoginWindow() throws HeadlessException {
        messageConsumer = new MessageConsumer("coordinator");
        currentlyActiveChats.add("default");
        this.setPreferredSize(new Dimension(300, 165));
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        screenWidth = screenSize.width;
        screenHeight = screenSize.height;
        int x = (screenWidth - 300) / 2;
        int y = 300;

        this.setLocation(x, y);

        System.out.println(screenHeight + "  " + y);
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
                            if (m.value().startsWith("logout")) {
                                String user = m.value().substring(7);
                                currentlyLoggedUsers.remove(user);
                            } else if (m.value().startsWith("create")) {
                                String chatAndUsers = m.value().substring(7);
                                StringBuilder sbd = new StringBuilder();
                                for (int i = 0; i < chatAndUsers.length(); i++) {
                                    if(chatAndUsers.charAt(i)==' '){
                                        break;
                                    } else{
                                        sbd.append(chatAndUsers.charAt(i));
                                    }
                                }
                                currentlyActiveChats.add(sbd.toString());
                            } else if (m.value().startsWith("login")) {
                                String user = m.value().substring(6);
                                System.out.println(this.currentlyLoggedUsers);
                                System.out.println(this.currentlyActiveChats);
                                System.out.println("users:" + currentlyLoggedUsers + "chats:" + currentlyActiveChats);
                                MessageProducer.send(new ProducerRecord<>("metadata", "users:" + currentlyLoggedUsers + "chats:" + currentlyActiveChats));
                            } else if(m.value().startsWith("fetch users&chats")){
                                String user = m.value().substring(18);
                                Set<String> activeChatsForUser = userChats.get(user);
                                MessageProducer.send(new ProducerRecord<>("metadata", "users:" + currentlyLoggedUsers + "chats:" + activeChatsForUser));
                            }
                        });
            }
        });
        loginField.addActionListener(e -> {
            passwordField.requestFocus();
        });
        passwordField.addActionListener(e -> {
            login();
        });
        signInButton.addActionListener(e -> {
            login();
        });
        registerButton.addActionListener(e -> {
            register();
        });
        forgotPasswordbutton.addActionListener(e -> {
            JOptionPane.showMessageDialog(this, "I dont know, but that doesn't change the fact that in Australia there are 48 million kangaroos and in Uruguay there are 3,457,480 inhabitants, so if kangaroos decide to invade Uruguay, each Uruguayan will have to fight 14 kangaroos");
        });
    }
    private void login() {
        if(!isUserInputCorrect()){
            return;
        }
        if(!areCredentialsCorrect()){
            return;
        }
        userChats.putIfAbsent(loginField.getText(),new HashSet<>());
        userChats.get(loginField.getText()).add("default");
        SwingUtilities.invokeLater(() -> {
            int chatWindowPosition = getChatWindowPosition();
            amountOfChatWindows++;
            new ChatWindow(loginField.getText(), chatWindowPosition);
            currentlyLoggedUsers.add(loginField.getText());
            MessageProducer.send(new ProducerRecord<>("metadata","login "+loginField.getText()));
            loginField.setText("");
            passwordField.setText("");
        });
    }
    private void register() {
        // moze byc tak ze tylko raz bedzie poprawnie
        if(!isUserInputCorrect()){
            return;
        }
        String login = loginField.getText();
        String password = new String(passwordField.getPassword());
        long count = userCredentials.stream().filter(c -> c.getLogin().equals(login)).count();
        if(count>0){
            JOptionPane.showMessageDialog(this,"A user with this username exists already.");
        } else{
            userCredentials.add(new UserCredentials(login, password));
            loginField.setText("");
            passwordField.setText("");
            JOptionPane.showMessageDialog(this,"Registered successfully");
        }
    }
    private boolean isUserInputCorrect() {
        if (loginField.getText().isEmpty() ||
                loginField.getText() == null) {
            JOptionPane.showMessageDialog(this, "Insert password.");
            return false;
        }
        if (loginField.getText().isEmpty() ||
                loginField.getText() == null) {
            JOptionPane.showMessageDialog(this, "Insert username.");
            return false;
        } else if (loginField.getText().contains(",")) {
            JOptionPane.showMessageDialog(this, "Username can not contain {','}.");
            return false;
        }
        return true;
    }



    private boolean areCredentialsCorrect() {
        String login = loginField.getText();
        String password = new String(passwordField.getPassword());
        for (UserCredentials userCredential : userCredentials) {
            if (userCredential.getLogin().equals(login) && userCredential.getPassword().equals(password)) {
                return true;
            }
        }
        JOptionPane.showMessageDialog(this, "Wrong login or password.");
        return false;

    }




}
