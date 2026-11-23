package com.engine.voucher.mq;

import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.entity.VoucherOrder;
import com.engine.voucher.mapper.VoucherMapper;
import com.engine.voucher.mapper.VoucherOrderMapper;
import com.engine.voucher.service.VoucherOrderService;
import com.engine.voucher.service.VoucherOrderServiceImpl;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutConsumer {

    private final VoucherOrderMapper voucherOrderMapper;
    private final VoucherMapper voucherMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final VoucherOrderServiceImpl voucherOrderServiceImpl;
    private final VoucherOrderService voucherOrderService;

    @RabbitListener(queues = RabbitMqConfig.ORDER_TIMEOUT_DLQ)
    public void handleOrderTimeout(String orderIdStr, Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        Long orderId = Long.valueOf(orderIdStr);

        try {
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
            // 2.1 MySQL 退库存：voucherMapper.refundStock(order.getVoucherId());
            // 2.2 Redis 退库存：stringRedisTemplate.opsForValue().increment("seckill:stock:" + order.getVoucherId());
            // 2.3 Redis 移除已购名单：stringRedisTemplate.opsForSet().remove("seckill:order:" + order.getVoucherId(), order.getUserId().toString());
//            voucherMapper.refundStock(order.getVoucherId());
//            stringRedisTemplate.opsForValue().increment("seckill:stock:" + order.getVoucherId());
//            stringRedisTemplate.opsForSet().remove("seckill:order:" + order.getVoucherId(), order.getUserId().toString());
            Long removed = stringRedisTemplate.opsForSet().remove("seckill:order:" + order.getVoucherId(), order.getUserId().toString());
            if (removed != null && removed == 1L) {
                stringRedisTemplate.opsForValue().increment("seckill:stock:" + order.getVoucherId());
            }
            channel.basicAck(deliveryTag, false);
            log.info("订单超时未核销，已自动退还库存，orderId={}", orderId);

        } catch (Exception e) {
            log.error("处理超时订单异常，orderId={}", orderId, e);
            channel.basicNack(deliveryTag, false, false);
        }
    }
}