package bcr.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MyMessageProducer implements Runnable, ExceptionListener {

    private Session session;

    private Connection connection;

    private String brokerUrl;

    private String queueName;

    private int msDelay;

    private int messagesProduced;

    public MyMessageProducer(String brokerUrl, String queueName, int msDelay) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
        this.msDelay = msDelay;
    }

    public void run() {
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);

            // Create a Connection
            connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queueName);

            // Create a MessageProducer from the Session to the Topic or Queue
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            while (true) {
                        // Create a messages
                        String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                        TextMessage message = null;
                        try {
                            message = session.createTextMessage(text);
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }

                        // Tell the producer to send the message
                        try {
                            producer.send(message);
                            messagesProduced++;
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                        try {
                            TimeUnit.MILLISECONDS.sleep(msDelay);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

            }


        } catch (Exception e) {
        }
    }

    @Override
    public void onException(JMSException e) {
        // Clean up
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

    public int getMessagesProduced() {
        return messagesProduced;
    }

    public void setMessagesProduced(int messagesProduced) {
        this.messagesProduced = messagesProduced;
    }
}
