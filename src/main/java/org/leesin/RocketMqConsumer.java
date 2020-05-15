package org.leesin;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class RocketMqConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer=
                new DefaultMQPushConsumer("gp_consumer_group");
        consumer.setNamesrvAddr("192.168.13.102:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("gp_test_topic","*");
        //并行消费
        /*consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                            ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("Receive Message: "+list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //签收
            }
        });*/
        //顺序消费，通过这个能实现顺序的机制
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {

                MessageExt  messageExt=list.get(0);
                //接受到的消息
                System.out.println("recieve message"+list);
                //TODO  --
                // Throw Exceptio
                // 重新发送该消息
                // DLQ（通用设计） 衰减重试16次 还是不行，就丢入DLQ死信队列
                if(messageExt.getReconsumeTimes()==3){  //消息重发了三次
                    //持久化 消息记录表
                    return ConsumeOrderlyStatus.SUCCESS; //签收  表示消息消费成功
                }
                return ConsumeOrderlyStatus.SUCCESS; //签收

            }
        });

        consumer.start();

    }
}
