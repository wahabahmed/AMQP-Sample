package Utils;

import RabbitMq.RabbitClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RabbitMqConsumingInvoker extends DefaultConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConsumingInvoker.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Channel channel;
    private ApplicationContext context;
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private RabbitClient rabbitClient;

    public RabbitMqConsumingInvoker(RabbitClient rabbitClient, ApplicationContext context, Channel channel) {
        super(channel);
        this.channel = channel;
        this.context = context;
        this.rabbitClient = rabbitClient;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
                               AMQP.BasicProperties properties, final byte[] body) throws IOException {
        LOGGER.info("Received Message ");
        LOGGER.info("consumerTag " + consumerTag);
        LOGGER.info("envelope " + envelope);
        LOGGER.info("properties " + properties);
        LOGGER.info("Body " + parseBody(body));
        long deliveryTag = envelope.getDeliveryTag();
        channel.basicAck(deliveryTag, false);
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                RemoteCommand remoteCommand = parseBody(body);
                try {
                    if (isMessageForMe(remoteCommand)) {
                        if (context != null) {
                            Class<?> targetClass = Class.forName(remoteCommand
                                    .getTarget());
                            Object bean = context.getBean(targetClass);
                            invokeWithArguments(bean, remoteCommand.getMethod(),
                                    remoteCommand.getParams());
                        } else {
                            LOGGER.error("No spring context available");
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("AMQP invocation failed", e);
                }
            }

        }, 0, TimeUnit.SECONDS);
    }

    private Object invokeWithArguments(Object bean, String methodName,
                                       Map<String, Object> params) {
        Object result = null;
        boolean withParams = false;

        try {
            Method method;
            if (bean instanceof RemoteInvokable && methodName.startsWith("invoke")) {
                if (params != null) {
                    method = ReflectionUtils.findMethod(bean.getClass(), "invokeWithParameters", Map.class);
                    withParams = true;
                } else {
                    method = ReflectionUtils.findMethod(bean.getClass(), "invoke");
                }
            } else {
                method = ReflectionUtils.findMethod(bean.getClass(), methodName);
            }

            if (withParams) {
                result = ReflectionUtils.invokeMethod(method, bean, params);
            } else {
                result = ReflectionUtils.invokeMethod(method, bean);
            }
        } catch (SecurityException e) {
            LOGGER.error("Error invoking command", e);
        }
        return result;
    }

    private boolean isMessageForMe(RemoteCommand remoteCommand) {
        return (remoteCommand != null
                && remoteCommand.getBindingKey() != null
                && (remoteCommand.getBindingKey().equals(RabbitClient.BINDING_KEY_GLOBAL)
                || remoteCommand.getBindingKey().equals(rabbitClient.getBindingKeySpecific())));
    }

    private RemoteCommand parseBody(byte[] body) {
        RemoteCommand remoteCommand = null;
        try {
            remoteCommand = OBJECT_MAPPER.readValue(body, RemoteCommand.class);
            String values = new ObjectMapper().writeValueAsString(remoteCommand);
            LOGGER.info("RemoteCommand? " + values);
        } catch (IOException e) {
            LOGGER.error("AMQP payload unreadable");
        }

        return remoteCommand;
    }
}
