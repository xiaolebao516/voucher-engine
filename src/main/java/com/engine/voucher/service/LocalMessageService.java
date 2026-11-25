package com.engine.voucher.service;

import cn.hutool.core.util.IdUtil;
import com.baomidou.mybatisplus.extension.service.IService;
import com.engine.voucher.constant.TraceConstant;
import com.engine.voucher.entity.LocalMessage;
import com.engine.voucher.mapper.LocalMessageMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class LocalMessageService {

    private static final int MAX_RETRY_COUNT = 5;
    private static final long BASE_BACKOFF_SECONDS = 10L;
    private static final long MAX_BACKOFF_SECONDS = 600L;

    private final LocalMessageMapper localMessageMapper;
    private final RabbitTemplate rabbitTemplate;

    /**
     * 在当前业务事务内写入本地消息，并注册事务提交后的即时投递回调
     */
    public void saveAndSendAfterCommit(String bizType, String bizId,
                                       String exchange, String routingKey, String payload) {
        LocalMessage msg = new LocalMessage();
        msg.setId(IdUtil.getSnowflakeNextId());
        msg.setBizType(bizType);
        msg.setBizId(bizId);
        msg.setExchange(exchange);
        msg.setRoutingKey(routingKey);
        msg.setPayload(payload);
        msg.setTraceId(MDC.get(TraceConstant.TRACE_ID_KEY));
        msg.setStatus(0);
        msg.setRetryCount(0);
        msg.setNextRetryTime(LocalDateTime.now());

        localMessageMapper.insert(msg);

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    sendMessage(msg);
                }
            });
        } else {
            sendMessage(msg);
        }
    }

    public void sendMessage(LocalMessage msg) {
        // 发消息异常需要sendFailed，如果不做异常处理，会把异常抛出，但回调时，事务已经提交，消息会发2次
        try {
            rabbitTemplate.convertAndSend(msg.getExchange(), msg.getRoutingKey(), message -> {
                message.getMessageProperties().setMessageId(String.valueOf(msg.getId()));
                if (msg.getTraceId() != null) {
                    message.getMessageProperties().setHeader(TraceConstant.TRACE_ID_HEADER, msg.getTraceId());
                }
                return message;
            });
            // 投递成功，修改下游状态，但不原子，实在失败，靠幂等兜底，起码不丢消息
            localMessageMapper.markSent(msg.getId());
        } catch (Exception e) {
            // 投递失败，先做延迟重试，指数退避，超过最大重试次数，置为Failed
            int currentRetry = msg.getRetryCount() == null ? 0 :  msg.getRetryCount();
            long delaySeconds = Math.min(BASE_BACKOFF_SECONDS * (1L << Math.min(currentRetry, 10)), MAX_BACKOFF_SECONDS);
            LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(delaySeconds);
            localMessageMapper.markFailed(MAX_RETRY_COUNT, nextRetryTime, msg.getId());
            log.error("本地消息投递MQ失败，等待下次重试，msgId={}, bizId={}, retryCount={}",
                    msg.getId(), msg.getBizType(), currentRetry + 1, e);
        }
    }

}
