package com.lyj.rabbitmq;

import com.lyj.rabbitmq.rabbitmqUtils.MqConnectionFactory;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.HashMap;


/**https://blog.csdn.net/nuoWei_SenLin/article/details/81457891
 * @author Sir_小三
 * @date 2019/11/12--23:05
 * 消费者，获得连接，创建通道，声明队列，创建消费者
 * mq限流，等待消费者完成通知mq队列，mq在发送下一条给消费者
 * 1.消费者 开启手动确认消息 channel.basicConsume(QUEUE_NAME,false,defaultConsumer);
 * 2. channel.basicQos(0,1,false);
 * 3.消息处理完在通知mq队列
 */
public class MqConsumer {
    public static  final String QUEUE_NAME="test1";
    public static void main(String[] args) throws Exception {
        //通过工厂获得rabbitmq的连接
        Connection connection = MqConnectionFactory.getConnection();
        //创建通道（rabbitmq消息的发送和消息的接收都要通过channel来完成）
        Channel channel = connection.createChannel();
        //交换机name
        String exchangeName="test_confirm_exchange";
        //路由routingKey
        String routingKey="test.#";
        //声明交换机 1.交换机name，2.交换机类型，3.是否持久化
        channel.exchangeDeclare(exchangeName,"topic",true);
        //声明死信
        HashMap<String, Object> agruments = new HashMap<>();
        //！！！注意如果队列已经存在，要删除，在此创建，（已经存在的队列不可改变），死信队列的key是固定的x-dead-letter-exchange
        agruments.put("x-dead-letter-exchange", "dlx.exchange");
        //声明一个队列
        //String var1, boolean var2, boolean var3, boolean var4, Map<String, Object> var5
        //1.代表队列2.代表是否持久化设为true那么rabbitmq服务关闭，数据也不会丢失
        //3.设为true代表只有这一个channel可以连接（独占）
        //4.自动删除，如果该队列没有任何订阅的消费者的话，该队列会被自动删除。这种队列适用于临时队列。
        //5.死信队列（通过此队列绑定的arguments上的交换机dlx.exchange，将此队列中的死信消息路由到dlx.exchange交换机所绑定的队列上）
        channel.queueDeclare(QUEUE_NAME,true,false,false,agruments);
        //绑定交换机和队列，以及routingkey
        channel.queueBind(QUEUE_NAME,exchangeName,routingKey);
        //声明死信交换机,队列，key，并进行绑定
        channel.exchangeDeclare("dlx.exchange","topic",true,false,null);
        channel.queueDeclare("dlx.queue",true,false,false,null);
        channel.queueBind("dlx.queue","dlx.exchange","#");
        //设置限流，1数据的大小，2每次发送数据的条数，3此设置范围消费者级别
        //限流是对于mq队列和消费者来说（发送者发送消息到mq队列，消费者处理不过来，那么就需要限流，等消费者完成，mq发送下一条）
        channel.basicQos(0,1,false);
        //消费者回调，消费者，还有一种优雅消费，解耦，自己去继承DefaultConsumer,重写handleDelivery方法，实现消费者
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    Thread.sleep(2000);
                }catch (Exception e){
                }finally {
                    //返回nack，第三个参数若设置为true，就会重回队列，false不回，成为死信，ttl过期时间变为死信，
                    //（一般都会采用死信队列来处理消费失败的消息，日志，人工干预，不会进行重回队列）
                    // 队列达到最大，存放不下，消息变为死信
                    // ！！！！！！经测试重回的消息不会添加到队列的尾部，而是头部
                    if((Integer) properties.getHeaders().get("num")==1){
                        System.out.println("num=1消息消费失败："+new String(body));
                        //1个参数，消息的唯一标签
                        //2是否批量处理，false不是
                        //3：false为死信，如果此时队列没有绑定死信队列，那么当消息变为死信时，找不到队列，会丢失这条消息
                        //死信消息一般为无法消费消息，可以记录日志查看，人工补偿
                        channel.basicNack(envelope.getDeliveryTag(),false,false);


                    }else {
                        System.out.println("正常消费消息："+new String(body));
                        //ack通知mq服务器，处理完成，可以发送下一条消息
                        //第二个参数是否批量处理，false不是
                        channel.basicAck(envelope.getDeliveryTag(),false);
                   }

                }
            }
        };
        //设置channel  1.队列名称2.是否自动确认消息(false的话可以做限流，)，3.消费者
        //获取消息，发起监听
        channel.basicConsume(QUEUE_NAME,false,defaultConsumer);

    }
}
