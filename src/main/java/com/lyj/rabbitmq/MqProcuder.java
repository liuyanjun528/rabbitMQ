package com.lyj.rabbitmq;

import com.lyj.rabbitmq.rabbitmqUtils.MqConnectionFactory;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;


/**
 * @author Sir_小三
 * @date 2019/11/12--23:06
 * 生产者，获得连接，创建通道，发送消息（如果是jms的话，jms需要创建一个消息的发送者来发送消息，而rabbitmq不需要，直接通过channel来发送消息）
 * 通道将消息发送到交换机
 */
public class MqProcuder {

    public static void main(String[] args) throws Exception {
        //通过工厂获得rabbitmq的连接
        Connection connection = MqConnectionFactory.getConnection();
        //创建通道（rabbitmq消息的发送和消息的接收都要通过channel来完成）
        Channel channel = connection.createChannel();

        //指定消息的确认confirm模式(消息发送之后进行回调消息发送者给与通知)
        channel.confirmSelect();
        //声明交换机
        String exchangeName = "test_confirm_exchange";
        //声明路由routingKey
        String routingKey = "test.1";

//        添加return监听（若交换机，或者路由不可达，mq回调此方法，通知消息发送者）
//        Return 消息机制用于处理一个不可路由的消息。在某些情况下，如果我们在发送消息的时候，
//        当前的 exchange 不存在或者指定路由 key 路由不到，
//        这个时候如果我们需要监听这种不可达的消息，就要使用 Return 消息机制了。
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println(replyCode);
                System.out.println(replyText);
                System.out.println(exchange);
                System.out.println(routingKey);
                System.out.println(properties);
                System.out.println("return回调：" + new String(body));

            }
        });


//       （3） confirm模式确认消息：添加监听，消息发送以后，mq会回调此方法，返回消息是否成功失败
//       这种监听方式，broker端批量回传给发送者的ack消息并不是以固定的批量大小回传的
//        channel.addConfirmListener(new ConfirmListener() {
//            @Override
//            //long deliveryTag  消息唯一标签，boolean multiple是否批量的
//            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
//                System.out.println("====成功");
//            }
//
//            @Override
//            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
//                System.out.println("====失败");
//            }
//        });
        for (int i = 0; i < 5; i++) {
            String mes = "这是发送到test1队列的消息" + i;
            //发送消息附带额外参数，手动签收时，判断为1，返回nack，并且可以设置是否重回队列（当任务失败的时候，根据业务返回ack或nack）
            HashMap<String, Object> map = new HashMap<>();
            map.put("num", i);
            //设置2为持久化，1为不持久化
            AMQP.BasicProperties build = new AMQP.BasicProperties().builder().deliveryMode(2).contentEncoding("utf-8").headers(map).build();
            //发送消息
            //通过通道发送消息
            //第1个参数交换机exchange，第2个参数rontinkey，
            //生产者发送消息的时候必须指定一个exchange，如果不指定那么使用rabbitmq默认的 AMQP default交换机，
            //此交换机路由规则，会根据第二个参数routinkey和rabbitmq中有没有队列name相同的，如果相同就可以路由过去，否则消息丢失
            //发送消息的时候mq如果没有test1这个队列的时候，消息会丢失！，不会发送到mq

            //3 true  return 模式 ，设置为true会回调ReturnListener进行通知，false不会
            // mandatory 为 true时，如果消息路由不到，不会删除消息，监听器可接受到该消息，可做后续的处理；
            // mandatory 默认为 false ，如果消息路由不到， Broke 端会自动删除该消息。
            channel.basicPublish(exchangeName, routingKey, true, build, mes.getBytes());

            //channel.basicPublish(exchangeName,routingKey,null,mes.getBytes());
            if (channel.waitForConfirms()) {  //（1）confirm模式确认消息：  发一条确认一条，串行代码
                System.out.println("发送成功");//（2）可以批量确认，把if提取到for外面，消息发送完，在一次性确认，这样一条失败全部失败
            } else {
                System.out.println("发送失败");
            }

        }

//        不关闭连接，等待发送消息后mq的回调
//        channel.close();
//        connection.close();
    }

}
