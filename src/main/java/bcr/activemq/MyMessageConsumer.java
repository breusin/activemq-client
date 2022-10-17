package bcr.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MyMessageConsumer implements Runnable, ExceptionListener {
    private Session session;

    private Connection connection;

    private MessageConsumer consumer;

    private String brokerUrl;

    private String queueName;

    private int messagesReceived = 0;

    public MyMessageConsumer(String brokerUrl, String queueName) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    public void run() {
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);

            // Create a Connection
            connection = connectionFactory.createConnection();
            connection.start();
            connection.setExceptionListener(this);

            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queueName);

            // Create a MessageConsumer from the Session to the Topic or Queue
            consumer = session.createConsumer(destination);

            while (true) {
                // Wait for a message
                Message message = consumer.receive();

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    messagesReceived++;
                }
            }

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onException(JMSException e) {
        // Clean up
        try {
            consumer.close();
        } catch (JMSException e1) {
            e1.printStackTrace();
        }
        try {
            session.close();
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
        try {
            connection.close();
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
        System.out.println("Caught: " + e);
        e.printStackTrace();

    }

    public int getMessagesReceived() {
        return messagesReceived;
    }

    public void setMessagesReceived(int messagesReceived) {
        this.messagesReceived = messagesReceived;
    }
}