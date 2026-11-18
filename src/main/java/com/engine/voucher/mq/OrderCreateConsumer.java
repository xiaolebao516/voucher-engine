package com.engine.voucher.mq;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.entity.MqConsumeLog;
import com.engine.voucher.entity.VoucherOrder;
import com.engine.voucher.mapper.MqConsumeLogMapper;
import com.engine.voucher.mapper.VoucherMapper;
import com.engine.voucher.mapper.VoucherOrderMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.LocalDateTime;

import static com.engine.voucher.config.RabbitMqConfig.ORDER_DELAY_EXCHANGE;
import static com.engine.voucher.config.RabbitMqConfig.ORDER_DELAY_ROUTING_KEY;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreateConsumer {

    private final MqConsumeLogMapper mqConsumeLogMapper;
    private final VoucherOrderMapper voucherOrderMapper;
    private final VoucherMapper voucherMapper;
    private final RabbitTemplate rabbitTemplate;

    @RabbitListener(queues = RabbitMqConfig.ORDER_CREATE_QUEUE)
    @Transactional(rollbackFor = Exception.class)
    public void handleOrderCreate(String msgJson, Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        JSONObject json = JSONUtil.parseObj(msgJson);
        Long orderId = json.getLong("orderId");
        Long userId = json.getLong("userId");
        Long voucherId = json.getLong("voucherId");

        try {
            // TODO 1：消费端幂等性校验（防止断电重投导致同一条消息被重复消费）
            // 提示：用 orderId.toString() 作为 msgId，new 一个 MqConsumeLog 插入 mqConsumeLogMapper.insert(logRecord)
            // 如果抛出 DuplicateKeyException，说明这条消息已经处理过了！直接打印日志、手动 ACK 后 return;
            String msgId = orderId.toString();
            MqConsumeLog mqConsumeLog = new MqConsumeLog();
            mqConsumeLog.setMsgId(msgId);
            mqConsumeLog.setCreateTime(LocalDateTime.now());
            mqConsumeLog.setBizType("ORDER_CREATE");
            mqConsumeLogMapper.insert(mqConsumeLog);

            // TODO 2：真正写入 MySQL 订单表 + 扣减数据库真实库存
            // 提示：
            // 2.1 调用 voucherMapper.deductStock(voucherId);
            // 2.2 new 一个 VoucherOrder，填入 orderId、userId、voucherId、status = 0、createTime = LocalDateTime.now()，调用 voucherOrderMapper.insert(order);
            int updated = voucherMapper.deductStock(voucherId);
            if (updated == 0) {
                log.warn("扣件库存失败");
            }
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(orderId);
            voucherOrder.setUserId(userId);
            voucherOrder.setVoucherId(voucherId);
            voucherOrder.setStatus(0);
            voucherOrder.setCreateTime(LocalDateTime.now());
            voucherOrderMapper.insert(voucherOrder);
            // TODO 3：把 orderId 发送到延迟队列（ORDER_DELAY_EXCHANGE / ORDER_DELAY_ROUTING_KEY），30秒后检查是否核销
            // 提示：rabbitTemplate.convertAndSend(..., ..., orderId.toString());
            rabbitTemplate.convertAndSend(ORDER_DELAY_EXCHANGE, ORDER_DELAY_ROUTING_KEY, orderId.toString());
            // 全部落库成功后，发送手动 ACK 通知 MQ 彻底删除这条消息
            channel.basicAck(deliveryTag, false);
            log.info("异步下单落库成功，orderId={}", orderId);

        } catch (DuplicateKeyException e) {
            log.warn("检测到重复消费消息，直接幂等跳过，orderId={}", orderId);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("异步下单异常，拒绝消息并重新入队重试，orderId={}", orderId, e);
            // 第三个参数 requeue = true 表示让 MQ 把消息放回队列重试
            channel.basicNack(deliveryTag, false, false);
        }
    }
}