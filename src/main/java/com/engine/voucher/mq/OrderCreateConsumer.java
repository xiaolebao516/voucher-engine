package com.engine.voucher.mq;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.entity.MqConsumeLog;
import com.engine.voucher.entity.VoucherOrder;
import com.engine.voucher.filter.TraceIdFilter;
import com.engine.voucher.mapper.MqConsumeLogMapper;
import com.engine.voucher.mapper.VoucherMapper;
import com.engine.voucher.mapper.VoucherOrderMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.time.LocalDateTime;

import static com.engine.voucher.config.RabbitMqConfig.ORDER_DELAY_EXCHANGE;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_DELAY_ROUTING_KEY;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_DLQ_ROUTING_KEY;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_DLX_EXCHANGE;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_RETRY_30S_ROUTING_KEY;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_RETRY_5S_ROUTING_KEY;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_RETRY_EXCHANGE;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreateConsumer {

    private static final String RETRY_COUNT_HEADER = "x-retry-count";
    private static final String SECKILL_STOCK_PREFIX = "seckill:stock:";

    private final MqConsumeLogMapper mqConsumeLogMapper;
    private final VoucherOrderMapper voucherOrderMapper;
    private final VoucherMapper voucherMapper;
    private final RabbitTemplate rabbitTemplate;
    private final TransactionTemplate transactionTemplate;
    private final StringRedisTemplate stringRedisTemplate;

    private enum TxResult {
        SUCCESS,
        DUPLICATE_MSG,
        DUPLICATE_ORDER
    }

    @RabbitListener(queues = RabbitMqConfig.ORDER_CREATE_QUEUE)
    public void handleOrderCreate(String msgJson, Message message, Channel channel) throws IOException {
        Object traceIdObj = message.getMessageProperties().getHeader(TraceIdFilter.TRACE_ID_KEY);
        String traceId = traceIdObj != null ? traceIdObj.toString() : "NO_TRACE";
        MDC.put(TraceIdFilter.TRACE_ID_KEY, traceId);

        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            Long orderId;
            Long userId;
            Long voucherId;
            try {
                JSONObject json = JSONUtil.parseObj(msgJson);
                orderId = json.getLong("orderId");
                userId = json.getLong("userId");
                voucherId = json.getLong("voucherId");
                if (orderId == null || userId == null || voucherId == null) {
                    throw new IllegalArgumentException("消息缺少必要业务字段");
                }
            } catch (Exception parseEx) {
                log.error("非法下单消息，直接转入死信队列，msgJson={}", msgJson, parseEx);
                sendMessageWithHeaders(ORDER_DLX_EXCHANGE, ORDER_DLQ_ROUTING_KEY, msgJson, traceId, 0);
                channel.basicAck(deliveryTag, false);
                return;
            }

            try {
                TxResult txResult = transactionTemplate.execute(status -> {
                    MqConsumeLog mqConsumeLog = new MqConsumeLog();
                    mqConsumeLog.setMsgId(orderId.toString());
                    mqConsumeLog.setCreateTime(LocalDateTime.now());
                    mqConsumeLog.setBizType("ORDER_CREATE");
                    try {
                        mqConsumeLogMapper.insert(mqConsumeLog);
                    } catch (DuplicateKeyException e) {
                        return TxResult.DUPLICATE_MSG;
                    }

                    int updated = voucherMapper.deductStock(voucherId);
                    if (updated == 0) {
                        throw new IllegalStateException("数据库库存扣减失败，库存不足");
                    }

                    VoucherOrder voucherOrder = new VoucherOrder();
                    voucherOrder.setId(orderId);
                    voucherOrder.setUserId(userId);
                    voucherOrder.setVoucherId(voucherId);
                    voucherOrder.setStatus(0);
                    voucherOrder.setCreateTime(LocalDateTime.now());
                    try {
                        voucherOrderMapper.insert(voucherOrder);
                    } catch (DuplicateKeyException e) {
                        status.setRollbackOnly();
                        return TxResult.DUPLICATE_ORDER;
                    }

                    return TxResult.SUCCESS;
                });

                if (txResult == TxResult.DUPLICATE_MSG) {
                    log.warn("检测到重复消费消息，直接幂等跳过，orderId={}", orderId);
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                if (txResult == TxResult.DUPLICATE_ORDER) {
                    log.warn("检测到用户重复下单触发唯一索引冲突，已回滚数据库并补偿Redis库存，userId={}, voucherId={}", userId, voucherId);
                    stringRedisTemplate.opsForValue().increment(SECKILL_STOCK_PREFIX + voucherId);
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                sendMessageWithHeaders(ORDER_DELAY_EXCHANGE, ORDER_DELAY_ROUTING_KEY, orderId.toString(), traceId, 0);
                channel.basicAck(deliveryTag, false);
                log.info("异步下单落库成功，orderId={}", orderId);

            } catch (Exception bizEx) {
                int retryCount = getRetryCount(message);
                if (retryCount == 0) {
                    log.warn("异步下单异常，进入5秒延迟重试队列，orderId={}", orderId, bizEx);
                    sendMessageWithHeaders(ORDER_RETRY_EXCHANGE, ORDER_RETRY_5S_ROUTING_KEY, msgJson, traceId, 1);
                } else if (retryCount == 1) {
                    log.warn("异步下单再次异常，进入30秒延迟重试队列，orderId={}", orderId, bizEx);
                    sendMessageWithHeaders(ORDER_RETRY_EXCHANGE, ORDER_RETRY_30S_ROUTING_KEY, msgJson, traceId, 2);
                } else {
                    log.error("异步下单达到最大重试次数，转入死信队列，orderId={}", orderId, bizEx);
                    sendMessageWithHeaders(ORDER_DLX_EXCHANGE, ORDER_DLQ_ROUTING_KEY, msgJson, traceId, retryCount);
                }
                channel.basicAck(deliveryTag, false);
            }
        } finally {
            MDC.remove(TraceIdFilter.TRACE_ID_KEY);
        }
    }

    private int getRetryCount(Message message) {
        Object countObj = message.getMessageProperties().getHeader(RETRY_COUNT_HEADER);
        if (countObj instanceof Number) {
            return ((Number) countObj).intValue();
        }
        return 0;
    }

    private void sendMessageWithHeaders(String exchange, String routingKey, String payload, String traceId, int retryCount) {
        OrderTimeoutConsumer.sendMessageWithHeaders(exchange, routingKey, payload, traceId, retryCount, rabbitTemplate, RETRY_COUNT_HEADER);
    }
}