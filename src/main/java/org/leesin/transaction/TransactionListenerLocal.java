package org.leesin.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;



public class TransactionListenerLocal implements TransactionListener {

    private Map<String, Boolean> results = new ConcurrentHashMap<>();

    //执行本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        System.out.println("开始执行本地事务：" + o.toString()); //o
        String orderId = o.toString();
        //模拟数据库保存(成功/失败)
        boolean result = Math.abs(Objects.hash(orderId)) % 2 == 0;//模拟数据入库操作
        results.put(orderId, result); //
        return result ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.UNKNOW;
        // 这个返回状态表示告诉broker这个事务消息是否被确认，允许给到consumer进行消费
        // LocalTransactionState.ROLLBACK_MESSAGE 回滚
        //LocalTransactionState.UNKNOW 未知
    }

    //提供给事务执行状态检查的回调方法，给broker用的(异步回调）
    //如果回查失败，消息就丢弃
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String orderId = messageExt.getKeys();
        System.out.println("执行事务回调检查： orderId:" + orderId);
        boolean rs = results.get(orderId);
        System.out.println("数据的处理结果：" + rs); //只有成功/失败
        return rs ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
