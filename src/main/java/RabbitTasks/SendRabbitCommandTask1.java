package RabbitTasks;

import RabbitMq.RabbitClient;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import Utils.RabbitMqConsumingInvoker;

import java.io.IOException;

public class SendRabbitCommandTask1 {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.0.1");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.exchangeDeclare("Test", "topic");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "Test", "*.test.com");
        channel.queueBind(queueName, "Test", "wahab");

        String message = getMessage(argv);


        channel.basicConsume(queueName, false, new RabbitMqConsumingInvoker(new RabbitClient(), null, channel));
       /* channel.basicPublish("", TASK_QUEUE_NAME,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + message + "'");*/

        /*channel.close();
        connection.close();*/

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        });
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 1)
            return "Hello World!";
        return joinStrings(strings, " ");
    }

    private static String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuilder words = new StringBuilder(strings[0]);
        for (int i = 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}