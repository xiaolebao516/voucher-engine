package com.engine.voucher.mq;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.constant.TraceConstant;
import com.engine.voucher.entity.MqConsumeLog;
import com.engine.voucher.entity.VoucherOrder;
import com.engine.voucher.mapper.MqConsumeLogMapper;
import com.engine.voucher.mapper.VoucherOrderMapper;
import com.engine.voucher.service.LocalMessageService;
import com.engine.voucher.service.StockBucketService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;

import static com.engine.voucher.config.RabbitMqConfig.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreateConsumer {

    private static final String RETRY_COUNT_HEADER = "x-retry-count";
    private static final String SECKILL_STOCK_PREFIX = "seckill:stock:";

    private final MqConsumeLogMapper mqConsumeLogMapper;
    private final VoucherOrderMapper voucherOrderMapper;
    private final RabbitTemplate rabbitTemplate;
    private final StockBucketService stockBucketService;
    private final StringRedisTemplate stringRedisTemplate;
    private final LocalMessageService localMessageService;

    private static final String SECKILL_COMP_PREFIX = "seckill:comp:";
    private static final DefaultRedisScript<Long> COMPENSATE_STOCK_SCRIPT;

    static {
        COMPENSATE_STOCK_SCRIPT = new DefaultRedisScript<>();
        COMPENSATE_STOCK_SCRIPT.setLocation(new ClassPathResource("lua/seckill_compensate_stock.lua"));
        COMPENSATE_STOCK_SCRIPT.setResultType(Long.class);
    }

    private enum TxResult {
        SUCCESS,
        DUPLICATE_MSG,
        DUPLICATE_ORDER
    }

    // 固定并发度，仅作用于下单队列；用于 1/2/4 消费者对照实验。
    @RabbitListener(queues = RabbitMqConfig.ORDER_CREATE_QUEUE,
            concurrency = "${seckill.order-consumer.concurrency:1}")
    public void handleOrderCreate(String msgJson, Message message, Channel channel) throws IOException {
        Object traceIdObj = message.getMessageProperties().getHeader(TraceConstant.TRACE_ID_HEADER);
        String traceId = traceIdObj != null ? traceIdObj.toString() : "NO_TRACE";
        MDC.put(TraceConstant.TRACE_ID_KEY, traceId);

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
                TxResult txResult = stockBucketService.execute(voucherId, userId, (bucketId, status) -> {
                    MqConsumeLog mqConsumeLog = new MqConsumeLog();
                    mqConsumeLog.setMsgId(orderId.toString());
                    mqConsumeLog.setCreateTime(LocalDateTime.now());
                    mqConsumeLog.setBizType("ORDER_CREATE");
                    try {
                        mqConsumeLogMapper.insert(mqConsumeLog);
                    } catch (DuplicateKeyException e) {
                        return TxResult.DUPLICATE_MSG;
                    }

                    VoucherOrder voucherOrder = new VoucherOrder();
                    voucherOrder.setId(orderId);
                    voucherOrder.setUserId(userId);
                    voucherOrder.setVoucherId(voucherId);
                    voucherOrder.setStockBucketId(bucketId);
                    voucherOrder.setStatus(0);
                    voucherOrder.setCreateTime(LocalDateTime.now());
                    try {
                        voucherOrderMapper.insert(voucherOrder);
                    } catch (DuplicateKeyException e) {
                        status.setRollbackOnly();
                        return TxResult.DUPLICATE_ORDER;
                    }

                    localMessageService.saveAndSendAfterCommit(
                            "ORDER_TIMEOUT",
                            orderId.toString(),
                            ORDER_DELAY_EXCHANGE,
                            ORDER_DELAY_ROUTING_KEY,
                            orderId.toString()
                    );

                    // 将热点库存行的条件更新放在事务最后，减少持锁至提交的时间。
                    // 库存不足仍抛异常：订单、消费日志、本地消息一起回滚，afterCommit 不会执行。
                    stockBucketService.deduct(voucherId, bucketId);

                    return TxResult.SUCCESS;
                });

                if (txResult == TxResult.DUPLICATE_MSG) {
                    log.warn("检测到重复消费消息，直接幂等跳过，orderId={}", orderId);
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                if (txResult == TxResult.DUPLICATE_ORDER) {
                    Long compResult = stringRedisTemplate.execute(
                            COMPENSATE_STOCK_SCRIPT,
                            Arrays.asList(SECKILL_STOCK_PREFIX + voucherId, SECKILL_COMP_PREFIX + orderId),
                            "604800"
                            // 过期时间比活动时间长就可以
                    );
                    if (Long.valueOf(0L).equals(compResult)) {
                        log.warn("用户重复下单，已回滚DB并原子补偿Redis库存，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
                    } else if (Long.valueOf(1L).equals(compResult)) {
                        log.warn("用户重复下单，该订单已补偿过Redis库存，幂等跳过，orderId={}", orderId);
                    } else if (Long.valueOf(2L).equals(compResult)) {
                        log.warn("用户重复下单，但券库存Key已不存在(可能已下架)，跳过补偿，orderId={}, voucherId={}", orderId, voucherId);
                    } else {
                        throw new IllegalStateException("Redis库存补偿脚本返回为空");
                    }
                    channel.basicAck(deliveryTag, false);
                    return;
                }

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
            MDC.remove(TraceConstant.TRACE_ID_KEY);
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