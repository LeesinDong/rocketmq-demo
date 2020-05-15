package org.leesin;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;


public class RocketMqProducer {


    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //事务消息的时候会用到
        //生产者集群模式
        DefaultMQProducer producer=new DefaultMQProducer("gp_producer_group");
        producer.setNamesrvAddr("192.168.13.102:9876"); //它会从命名服务器上拿到broker的地址
        producer.start();

        int num=0;
        while(num<20){
            num++;
            //Topic
            //tags -> 标签 （分类） -> (筛选)
            Message message=new Message("gp_test_topic","TagA",("Hello , RocketMQ:"+num).getBytes());
            //没有回执
            // producer.sendOneway();

            //消息路由策略
            SendResult send = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    //发送到哪里
                    return list.get(0);
                }
            }, "key-" + num);  //这个key类似kafka的key
            //发送出去的消息
            System.out.println(send);
        }
    }
}
