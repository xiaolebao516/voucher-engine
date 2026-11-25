package com.engine.voucher.task;

import com.engine.voucher.entity.LocalMessage;
import com.engine.voucher.mapper.LocalMessageMapper;
import com.engine.voucher.service.LocalMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class LocalMessageRelayTask {

    private static final String RELAY_LOCK_KEY = "lock:local_message:relay";
    private static final int BATCH_SIZE = 100;

    private final RedissonClient redissonClient;
    private final LocalMessageMapper localMessageMapper;
    private final LocalMessageService localMessageService;

    /**
     * 每 3 秒扫描一次到点待发消息（fixedDelay 保证上一轮彻底结束才开始计时下一轮）
     */
    @Scheduled(fixedDelay = 3000)
    public void relayPendingMessages() {
        RLock lock = redissonClient.getLock(RELAY_LOCK_KEY);
        // 无参 tryLock：抢不到直接跳过本轮；抢到则自动开启看门狗续期
        if (!lock.tryLock()) {
            return;
        }
        try {
            List<LocalMessage> pendingList = localMessageMapper.selectPendingMessages(
                    LocalDateTime.now(), BATCH_SIZE);
            if (pendingList.isEmpty()) {
                return;
            }
            for (LocalMessage msg : pendingList) {
                localMessageService.sendMessage(msg);
            }
        } finally {
            // 防止极端 GC 超时锁已易主时误删或抛异常
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}