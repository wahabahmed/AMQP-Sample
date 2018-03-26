package RabbitMq;

import Utils.RabbitMqConsumingInvoker;
import Utils.RemoteCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.SchedulingTaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Observable;

@SpringBootApplication
@EnableAsync
@Service
public class RabbitClient extends Observable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitClient.class);

    public static final String BINDING_KEY_GLOBAL = "*.test.com";
    private static final int CONNECTION_TIMEOUT = 30000;
    private static final int HEARTBEAT_INTERVAL = 120000;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    /**
     * Set this to null by default. Use the getBindingKeySpecific method to init the Binding Key value
     */
    private String bindingKeySpecific = null;
    private ApplicationContext applicationContext;
    private SchedulingTaskExecutor taskExecutor;
    @Value("${amqp.enabled:false}")
    private Boolean enabled = Boolean.FALSE;
    @Value("${amqp.host:192.168.0.1}")
    private String host;
    @Value("${amqp.port:5672}")
    private int port;
    @Value("${amqp.exchange.name:queueExchangeName}")
    private String exchangeName;
    @Value("${amqp.exchange.type:topic}")
    private String exchangeType;
    @Value("${amqp.exchange.binding.key:*.key.com}")
    private String bindingKey;
    @Value("${amqp.exchange.message.expiry:604800000}")
    private long expiry = 604800000;
    private ConnectionFactory connectionFactory;
    private Channel channel;
    private Connection connection;


    public RabbitClient() {
    }

    public RabbitClient(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.enabled = builder.enabled;
        this.exchangeName = builder.exchangeName;
        this.exchangeType = builder.exchangeType;
        this.bindingKey = builder.bindingKey;
        this.expiry = builder.expiry;
    }

    public static void main(String[] args) {
        RabbitClient client =
                new RabbitClient.Builder("192.168.0.1", 5671)
                        .bindingKey("*.test.com")
                        .enabled(true)
                        .exchangeName("jukebookclients")
                        .exchangeType("topic")
                        .build();
        client.setEnabled(true);
        client.init();
    }

    private String getHostname() {
        try {
            return "wahab";
        } catch (Exception e) {
            LOGGER.error("Error getting SiteId Hostname for AmqpClient. Attempting to get PC Name instead...");
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException ex) {
                LOGGER.error("Error getting PC Hostname for AmqpClient. Returning unknownhost.test.host instead...");
                return "unknownhost.test.host";
            }
        }
    }

    @Scheduled(initialDelay = 30 * 1000, fixedDelay = 60 * 1000)
    private void keepalive() {
        if (enabled && !hasConnection()) {
            LOGGER.warn("AMQP Channel Closed : calling init");
            close();
            init();
        }
    }

    public String getBindingKeySpecific() {
        if (bindingKeySpecific == null) {
            LOGGER.warn("AmqpClient Hostname is null. Getting Hostname...");
            bindingKeySpecific = getHostname();
        }
        LOGGER.info("AmqpClient Hostname = " + this.bindingKeySpecific);
        return bindingKeySpecific;
    }

    public boolean hasConnection() {
        return channel != null && channel.isOpen();
    }

    @PostConstruct
    public void init() {
        final RabbitClient rabbitClient = this;
        if (enabled) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    LOGGER.info("Initialising AMQP (RabbitMQ)...");
                    System.out.println("Initialising AMQP (RabbitMQ)...");
                    try {
                        connectionFactory = new ConnectionFactory();
                        connectionFactory.setHost(host);
                        connectionFactory.setPort(port);
                        connectionFactory.setConnectionTimeout(CONNECTION_TIMEOUT);
                        connectionFactory.setRequestedHeartbeat(HEARTBEAT_INTERVAL);

                        connection = getConnection();

                        getChannel().exchangeDeclare(exchangeName, exchangeType);

                        String queueName = channel.queueDeclare().getQueue();
                        channel.queueBind(queueName, exchangeName, BINDING_KEY_GLOBAL);
                        channel.queueBind(queueName, exchangeName, getBindingKeySpecific());

                        channel.basicConsume(queueName, false, new RabbitMqConsumingInvoker(rabbitClient, applicationContext, channel));
                        LOGGER.info("AMQP Command Receiver Registered");
                    } catch (SocketTimeoutException e) {
                        LOGGER.error("AMQP is not available at the moment.");
                    } catch (SocketException socketException) {
                        LOGGER.error(socketException.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("AMQP initialisation problem, no command interface available.", e);
                    }
                    LOGGER.info("Finished Initialising AMQP (RabbitMQ). Notifying observers...");
                    setChanged();
                    notifyObservers();
                }
            };

            TaskExecutor taskExecutor = new TaskExecutor() {
                @Override
                public void execute(Runnable task) {

                }
            };

            if (taskExecutor != null) {
                taskExecutor.execute(r);
            } else {
                LOGGER.error("Error running task to initialise AMQP (RabbitMQ) because taskExecutor is NULL.");
            }
        } else {
            LOGGER.warn("Skipping initialisation of AMQP (RabbitMQ) because AMQP is disabled.");
        }
    }

    @PreDestroy
    public void close() {
        if (enabled) {
            try {
                channel.close();
                connection.close();
            } catch (Exception e) {
                LOGGER.warn("Exception while closing amqp client connection");
            }
        }
    }

    public void send(RemoteCommand remoteCommand) throws IOException {
        if (enabled) {
            String message = OBJECT_MAPPER.writeValueAsString(remoteCommand);
            channel.basicPublish(exchangeName, remoteCommand.getBindingKey(), null,
                    message.getBytes());
            LOGGER.info("AMQP Sent : " + message);
        }
    }

    public void send(String bindingKey, AMQP.BasicProperties properties, byte[] payload) throws IOException {
        if (enabled) {
            channel.basicPublish(exchangeName, bindingKey, properties, payload);
        }
    }

    private Channel getChannel() throws IOException {
        if (channel == null || !channel.isOpen()) {
            channel = getConnection().createChannel();
        }
        return channel;
    }

    private Connection getConnection() throws IOException {
        if (connection == null || !connection.isOpen()) {
            connection = connectionFactory.newConnection();
        }
        return connection;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    @Autowired
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Autowired
    public void setSchedulingTaskExecutor(@Qualifier(value = "taskExecutor") SchedulingTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public static class Builder {

        private Boolean enabled = Boolean.FALSE;
        private String host;
        private int port;

        private String exchangeName;
        private String exchangeType;
        private String bindingKey;
        private long expiry;

        private String trustStoreClassPath;
        private String trustStorePassword;
        private String keyStoreClassPath;
        private String keyStorePassword;

        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder exchangeName(String exchangeName) {
            this.exchangeName = exchangeName;
            return this;
        }

        public Builder exchangeType(String exchangeType) {
            this.exchangeType = exchangeType;
            return this;
        }

        public Builder bindingKey(String bindingKey) {
            this.bindingKey = bindingKey;
            return this;
        }

        public Builder expiry(long expiry) {
            this.expiry = expiry;
            return this;
        }

        public Builder trustStoreClassPath(String trustStoreClassPath) {
            this.trustStoreClassPath = trustStoreClassPath;
            return this;
        }

        public Builder trustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        public Builder keyStoreClassPath(String keyStoreClassPath) {
            this.keyStoreClassPath = keyStoreClassPath;
            return this;
        }

        public Builder keyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        public RabbitClient build() {
            return new RabbitClient(this);
        }
    }
}
