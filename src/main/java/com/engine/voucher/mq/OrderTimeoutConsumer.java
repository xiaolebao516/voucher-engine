package com.engine.voucher.mq;

import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.constant.TraceConstant;
import com.engine.voucher.entity.VoucherOrder;
import com.engine.voucher.mapper.VoucherOrderMapper;
import com.engine.voucher.service.VoucherOrderService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;

import static com.engine.voucher.config.RabbitMqConfig.ORDER_DLQ_ROUTING_KEY;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_DLX_EXCHANGE;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutConsumer {
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT_ROLLBACK;
    private static final String RETRY_COUNT_HEADER = "x-retry-count";

    static {
        SECKILL_SCRIPT_ROLLBACK = new DefaultRedisScript<>();
        SECKILL_SCRIPT_ROLLBACK.setLocation(new ClassPathResource("lua/seckill_rollback.lua"));
        SECKILL_SCRIPT_ROLLBACK.setResultType(Long.class);
    }

    private final VoucherOrderMapper voucherOrderMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final VoucherOrderService voucherOrderService;
    private final RabbitTemplate rabbitTemplate;


    @RabbitListener(queues = RabbitMqConfig.ORDER_TIMEOUT_DLQ)
    public void handleOrderTimeout(String orderIdStr, Message message, Channel channel) throws IOException {
        Object traceIdObj = message.getMessageProperties().getHeader(TraceConstant.TRACE_ID_HEADER);
        String traceId = traceIdObj != null ? traceIdObj.toString() : "NO_TRACE";
        MDC.put(TraceConstant.TRACE_ID_KEY, traceId);

        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Long orderId = null;
        try {
            try {
                orderId = Long.valueOf(orderIdStr);
            } catch (IllegalArgumentException e){
                log.error("参数非法", e);
                sendMessageWithHeaders(ORDER_DLX_EXCHANGE, ORDER_DLQ_ROUTING_KEY, orderIdStr, traceId, 0);
                channel.basicAck(deliveryTag, false);
                return;
            }
            VoucherOrder order = voucherOrderMapper.selectById(orderId);
            if (order == null) {
                channel.basicAck(deliveryTag, false);
                return;
            }
            // TODO 1：CAS 乐观锁尝试将订单状态从 0(待核销) 改为 3(已过期退还)
            // 提示：调用 int updated = voucherOrderMapper.timeoutOrder(orderId);
            // 如果 updated == 0，说明订单已经被核销(status=2)或已经退过款(status=3)了，
            // 直接打一行 log.info("订单无需退还，orderId={}", orderId)，调用 channel.basicAck(deliveryTag, false); 然后 return;
//            int updated = voucherOrderMapper.timeoutOrder(orderId);
//            if (updated == 0) {
//                log.info("订单无需退换，orderId={}", orderId);
//                channel.basicAck(deliveryTag, false);
//                return;
//            }
            // 改后版本，将数据库事务操作单独写在service里，确定提交后才执行redis操作，防止sql回滚redis没改
            // 先改sql，落库提交，才安全的改Redis，签收mq
            boolean deRefunded = voucherOrderService.refundOrderInDb(orderId, order.getVoucherId());
            if (!deRefunded) {
                channel.basicAck(deliveryTag, false);
                return;
            }
            // TODO 2：确实超时未核销！把库存退回 MySQL 和 Redis，并解除该用户的一人一单限制
            // 提示（共 3 句）：
//          voucherMapper.refundStock(order.getVoucherId());
//          stringRedisTemplate.opsForValue().increment("seckill:stock:" + order.getVoucherId());
//          stringRedisTemplate.opsForSet().remove("seckill:order:" + order.getVoucherId(), order.getUserId().toString());
            try {
                stringRedisTemplate.execute(SECKILL_SCRIPT_ROLLBACK,
                        Arrays.asList("seckill:stock:" + order.getVoucherId(), "seckill:order:" + order.getVoucherId()),
                        order.getUserId().toString()
                );
            } catch (Exception redisEx) {
                log.error("Redis回滚异常，需要依赖定时对账修复，orderId={}, userId={}, voucherId={}", orderId, order.getUserId(), order.getVoucherId(), redisEx);
            }
            channel.basicAck(deliveryTag, false);
            log.info("订单超时未核销，已自动退还库存，orderId={}", orderId);

        } catch (Exception e) {
            log.error("处理超时订单异常，转入死信队列，orderId={}", orderId, e);
            sendMessageWithHeaders(ORDER_DLX_EXCHANGE, ORDER_DLQ_ROUTING_KEY, orderIdStr, traceId, 0);
            channel.basicAck(deliveryTag, false);
        } finally {
            MDC.remove(TraceConstant.TRACE_ID_KEY);
        }
    }
    private void sendMessageWithHeaders(String exchange, String routingKey, String payload, String traceId, int retryCount) {
        sendMessageWithHeaders(exchange, routingKey, payload, traceId, retryCount, rabbitTemplate, RETRY_COUNT_HEADER);
    }

    static void sendMessageWithHeaders(String exchange, String routingKey, String payload, String traceId, int retryCount, RabbitTemplate rabbitTemplate, String retryCountHeader) {
        rabbitTemplate.convertAndSend(exchange, routingKey, payload, msg -> {
            if (traceId != null && !"NO_TRACE".equals(traceId)) {
                msg.getMessageProperties().setHeader(TraceConstant.TRACE_ID_HEADER, traceId);
            }
            if (retryCount > 0) {
                msg.getMessageProperties().setHeader(retryCountHeader, retryCount);
            }
            return msg;
        });
    }

}