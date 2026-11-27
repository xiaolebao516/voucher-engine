package com.engine.voucher.config;

import com.engine.voucher.mapper.LocalMessageMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class RabbitConfirmConfig {

    private static final DefaultRedisScript<Long> SECKILL_ROLLBACK_SCRIPT;

    static {
        SECKILL_ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        SECKILL_ROLLBACK_SCRIPT.setLocation(new ClassPathResource("lua/seckill_rollback.lua"));
        SECKILL_ROLLBACK_SCRIPT.setResultType(Long.class);
    }

    private final RabbitTemplate rabbitTemplate;
    private final LocalMessageMapper localMessageMapper;
    private final StringRedisTemplate stringRedisTemplate;

    @PostConstruct
    public void initCallbacks() {
        // 1. 路由失败回调（消息进了 Exchange，但找不到匹配 Queue）
        rabbitTemplate.setReturnsCallback(returned ->
                log.error("[ALERT][MQ_RETURN] 消息路由失败被退回，exchange={}, routingKey={}, replyText={}, body={}",
                        returned.getExchange(), returned.getRoutingKey(), returned.getReplyText(),
                        new String(returned.getMessage().getBody(), StandardCharsets.UTF_8))
        );

        // 2. Broker 确认回调（注意：Spring AMQP 会先填好 correlationData.getReturned() 再调这里）
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (correlationData == null || correlationData.getId() == null) {
                if (!ack) {
                    log.error("[ALERT][MQ_NACK] 无业务ID消息发送失败，cause={}", cause);
                }
                return;
            }
            String id = correlationData.getId();
            boolean delivered = ack && correlationData.getReturned() == null;

            // 分支 A：本地消息表（超时关单等）
            if (id.startsWith("LOCAL_MSG:")) {
                Long msgId = Long.valueOf(id.substring("LOCAL_MSG:".length()));
                if (delivered) {
                    localMessageMapper.markSent(msgId);
                } else {
                    log.error("[ALERT][LOCAL_MSG_CONFIRM_FAIL] 本地消息投递未确认，msgId={}, ack={}, cause={}", msgId, ack, cause);
                    localMessageMapper.markFailed(5, LocalDateTime.now().plusSeconds(10), msgId);
                }
                return;
            }

            // 分支 B：秒杀下单消息（格式 SECKILL:orderId:voucherId:userId）
            if (id.startsWith("SECKILL:") && !delivered) {
                String[] parts = id.split(":");
                String orderId = parts[1], voucherId = parts[2], userId = parts[3];
                log.error("[ALERT][SECKILL_MQ_FAIL] 秒杀消息投递失败，回滚Redis库存，orderId={}, cause={}", orderId, cause);
                try {
                    stringRedisTemplate.execute(
                            SECKILL_ROLLBACK_SCRIPT,
                            Arrays.asList("seckill:stock:" + voucherId, "seckill:order:" + voucherId),
                            userId
                    );
                } catch (Exception e) {
                    log.error("[ALERT][SECKILL_ROLLBACK_FAIL] 确认失败后Redis回滚异常，需对账修复，orderId={}", orderId, e);
                }
            }
        });
    }
}