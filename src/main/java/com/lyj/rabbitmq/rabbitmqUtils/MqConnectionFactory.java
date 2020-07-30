package com.lyj.rabbitmq.rabbitmqUtils;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Sir_小三
 * @date 2019/11/12--23:00
 */
public class MqConnectionFactory {
    public static  Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setPassword("guest");
        connectionFactory.setUsername("guest");
        connectionFactory.setPort(5672);
        connectionFactory.setHost("122.51.212.154");
        connectionFactory.setVirtualHost("/");
        return connectionFactory.newConnection();

    }
}
