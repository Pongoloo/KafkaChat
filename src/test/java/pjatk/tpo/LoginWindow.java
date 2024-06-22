package pjatk.tpo;

import javax.swing.*;
import java.awt.*;

public class LoginWindow extends JFrame {
    private JTextField loginField;
    private JButton loginButton;
    private JPanel mainPanel;

    private int amountOfChatWindows =0;
    private int screenWidth;
    private int screenHeight;
    private int getChatWindowPosition(){
        if(100+500*(amountOfChatWindows+1) > screenWidth){
            amountOfChatWindows=0;
        }
        return 100+500*amountOfChatWindows;
    }
    public LoginWindow() throws HeadlessException {
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

        loginButton.addActionListener(e -> {
            if(loginField.getText().equals("") ||
            loginField.getText()==null){
                JOptionPane.showMessageDialog(this, "imo 30 punktow za to");
            } else{
                SwingUtilities.invokeLater( () ->{
                    int chatWindowPosition = getChatWindowPosition();
                    amountOfChatWindows++;
                    new ChatWindow(loginField.getText(), chatWindowPosition);
                    loginField.setText("");
                });
            }

        });
    }
}
