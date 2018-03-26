package Utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Created by wahaba on 14/12/2017.
 */
public class Receiver {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv)
            throws java.io.IOException,
            java.lang.InterruptedException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }
}
