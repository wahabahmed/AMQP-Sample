package RabbitTasks;

import RabbitMq.RabbitClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import Utils.RabbitMqConsumingInvoker;
import Utils.RemoteCommand;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class SendRabbitCommandTask2 {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.0.1");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.confirmSelect();

        channel.exchangeDeclare("Test", "topic");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "Test", "*.test.com");
        channel.queueBind(queueName, "Test", "waqas");


        RemoteCommand command = new RemoteCommand();
        command.setId("987");
        command.setType("type");
        command.setTarget("Utils.Greeting");
        command.setBindingKey("wahab");
        command.setMethod("main");
        command.setParams(null);
        String message = getMessage(argv);


        channel.basicConsume(queueName, false, new RabbitMqConsumingInvoker(new RabbitClient(), null, channel));

       /* channel.basicPublish("", TASK_QUEUE_NAME,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + message + "'");*/

        /*channel.close();
        connection.close();*/

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("Message Delivered? " + deliveryTag + " " + multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("Message Failed? " + deliveryTag + " " + multiple);
            }
        });

        channel.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                System.out.println("Shutdown");
            }
        });

        ObjectMapper mapper = new ObjectMapper();
        String body = "";
        try {
            body = mapper.writeValueAsString(command);
        } catch (Exception e) {

        }

        /***
         * Sending Message to SendRabbitCommandTask1
         */
        channel.basicPublish("Test", "wahab", null, body.getBytes("UTF-8"));
        channel.getFlow();
        System.out.println("published message");

        GetResponse response = channel.basicGet(queueName, false);
        channel.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {

            }
        });

        try {
            channel.waitForConfirmsOrDie();
        } catch (Exception e) {
            System.out.println("Exception");
            System.out.println("UNpublished message");
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    channel.close();
                    connection.close();
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

    private static boolean processMessage(QueueingConsumer.Delivery delivery) throws UnsupportedEncodingException {
        String msg = new String(delivery.getBody(), "UTF-8");
        System.out.println("[x] Rec: redeliver= " + delivery.getEnvelope().isRedeliver() + "");

        return true;

    }
}