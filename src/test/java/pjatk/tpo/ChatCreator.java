package pjatk.tpo;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ChatCreator extends JFrame{
    private JPanel panel1;
    private JButton createButton;
    private JList users;

    public ChatCreator(Set<String> userSet, String chatName,double x , double y, String creator) throws HeadlessException {
        this.setTitle("Chat creator");
        this.add(panel1);
        this.setLocation((int)x+50,(int)y-100);
        this.setPreferredSize(new Dimension(400, 300));
        this.pack();
        this.setVisible(true);
        DefaultListModel<String> xd = new DefaultListModel<>();

        users.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        DefaultListModel<String> listModel = new DefaultListModel<>();
        listModel.addAll(userSet);
        users.setModel(listModel);
        createButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                List<String> selectedValuesList = users.getSelectedValuesList();
                String usersString = "[";
                for (String s : selectedValuesList) {
                    usersString += s+",";
                }
                usersString = usersString.substring(0, usersString.length() - 1);
                usersString += "]";
                System.out.println("create "+chatName +
                        " users:"+usersString);
                MessageProducer.send(new ProducerRecord<>(
                                "metadata",
                                "create "+chatName + " " + creator +
                                        " users:"+usersString));
                dispose();
            }
        });
    }


}
