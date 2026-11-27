package com.engine.voucher.mq;

import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.constant.TraceConstant;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderDlqConsumer {

    @RabbitListener(queues = RabbitMqConfig.ORDER_DLQ_QUEUE)
    public void handleDeadLetter(String payload, Message message, Channel channel) throws IOException {
        Object traceIdObj = message.getMessageProperties().getHeader(TraceConstant.TRACE_ID_HEADER);
        String traceId = traceIdObj != null ? traceIdObj.toString() : "NO_TRACE";
        MDC.put(TraceConstant.TRACE_ID_KEY, traceId);

        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            // 统一告警前缀，方便日志系统抓捕并推送钉钉/飞书/邮件告警，生产可在此落库死信工单表
            log.error("[ALERT][MQ_DLQ] 收到死信消息，需人工介入排查，exchange={}, routingKey={}, payload={}",
                    message.getMessageProperties().getReceivedExchange(),
                    message.getMessageProperties().getReceivedRoutingKey(),
                    payload);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            // 死信队列已经是终点，捕获所有异常并签收，严防毒消息无限循环
            log.error("[ALERT][MQ_DLQ_HANDLE_ERR] 处理死信消息异常，已强制签收防死循环，payload={}", payload, e);
            channel.basicAck(deliveryTag, false);
        } finally {
            MDC.remove(TraceConstant.TRACE_ID_KEY);
        }
    }
}