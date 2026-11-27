package com.engine.voucher.task;

import com.engine.voucher.entity.LocalMessage;
import com.engine.voucher.mapper.LocalMessageMapper;
import com.engine.voucher.mapper.MqConsumeLogMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 历史数据清理任务：防止 tb_mq_consume_log / tb_local_message 无限增长。
 * 这两张表都是「只增不减」的记账表，不清理会一直涨。但它们的安全性要求完全不同：
 *   1. tb_mq_consume_log 是幂等表 —— 删早了会让重复消息被当成新消息再消费一次，
 *      直接造成重复扣库存。所以保留窗口必须远大于「消息在 MQ 里的最大存活时间」，
 *      包括两级重试队列的 5s/30s TTL、DLQ 里堆积的时间、以及人工从 DLQ 重放的时间。
 *      保守取 7 天。
 *   2. tb_local_message 是本地消息表 —— 只能删终态（status 1/2），status = 0 的
 *      是还没投出去的消息，删了就是丢单。同样保留 7 天，留出人工排查 status = 2 的窗口。
 * 实现上刻意不用「一条 DELETE 删到底」，而是小批量循环：
 * 大批量 DELETE 会长时间持有行锁、撑大 undo log、让主从延迟飙升，
 * 在线上表现为「凌晨三点数据库突然卡住」。分批删把压力摊平成若干个短事务。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataCleanupTask {

    /** 数据保留天数。改小之前先想清楚：幂等记录删早了会重复扣库存。 */
    private static final int RETENTION_DAYS = 7;

    /** 单批删除行数。太小则清理慢，太大则单事务变重。1000 是经验值。 */
    private static final int BATCH_SIZE = 1000;

    /** 批间停顿（毫秒）。给主从复制和 binlog 一点喘息时间，别把 IO 打满。 */
    private static final long BATCH_PAUSE_MILLIS = 50L;

    /** 防止某一轮清理跑太久，与下一轮重叠。 */
    private static final String CLEANUP_LOCK_KEY = "lock:task:data:cleanup";

    private final MqConsumeLogMapper mqConsumeLogMapper;
    private final LocalMessageMapper localMessageMapper;
    private final RedissonClient redissonClient;

    /**
     * 每天凌晨 3:30 执行。
     *
     * 为什么用 cron 而不是 fixedDelay：清理是「每天做一次」的运维动作，
     * 应该固定在业务低峰期跑，而不是从启动那一刻起每 24 小时跑一次
     * （那样重启一次时间就漂移一次，可能漂到白天高峰）。
     */
    @Scheduled(cron = "0 30 3 * * ?")
    public void cleanupExpiredData() {
        RLock lock = redissonClient.getLock(CLEANUP_LOCK_KEY);
        // 多实例部署时只让一台机器真正执行，避免几个节点同时删同一批数据
        // 无参 tryLock：抢不到就跳过本轮，等下一天
        if (!lock.tryLock()) {
            return;
        }
        try {
            LocalDateTime deadline = LocalDateTime.now().minusDays(RETENTION_DAYS);

            // ===== 任务 1：清理幂等记录 =====
            // 分批删的原因见类注释：一条 DELETE 删到底会长时间持锁、撑大 undo log。
            int consumeTotal = 0;
            while (true) {
                int deleted = mqConsumeLogMapper.deleteByCreateTimeBefore(deadline, BATCH_SIZE);
                consumeTotal += deleted;
                if (deleted < BATCH_SIZE) break;
                try {
                    Thread.sleep(BATCH_PAUSE_MILLIS);
                } catch (InterruptedException e) {
                    // 恢复中断标志，让上层（线程池关闭等）能感知到；不能吞掉
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            log.info("清理幂等记录完成，deadline={}, deleted={}", deadline, consumeTotal);

            // ===== 任务 2：告警「重试耗尽被放弃」的本地消息 =====
            // 必须在清理之前执行：status=2 且已过期的记录会在下一步被删掉，
            // 先删后扫的话这些记录就永远看不到了。
            //
            // 为什么必须单独告警：status=2 的消息从未成功投递给 broker，
            // RabbitMQ 里没有副本，死信队列也看不到 —— 只能查这张表。
            //
            // 为什么用「update_time > since」而不是「每轮都把 status=2 全捞出来」：
            // 否则同一条坏消息每天都会告警一次，直到有人修完或它被清理掉，
            // 变成「狼来了」，真出问题时反而没人看。用时间水位线只报新增的。
            List<LocalMessage> abandoned = localMessageMapper.selectAbandonedSince(
                    LocalDateTime.now().minusHours(24), 100);
            if (!abandoned.isEmpty()) {
                log.error("[ALERT][LOCAL_MSG_ABANDONED] 近 24 小时有 {} 条本地消息重试耗尽被放弃，需人工排查，首条 msgId={}",
                        abandoned.size(), abandoned.get(0).getId());
            }

            // ===== 任务 3：清理已终结的本地消息 =====
            // WHERE 里的 status IN (1,2) 写在 Mapper 的 SQL 里：
            // status=0 是还没投出去的消息，无论多老都不能删，删了就是丢单。
            int localTotal = 0;
            while (true) {
                int deleted = localMessageMapper.deleteFinishedBefore(deadline, BATCH_SIZE);
                localTotal += deleted;
                if (deleted < BATCH_SIZE) break;
                try {
                    Thread.sleep(BATCH_PAUSE_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            log.info("清理本地消息完成，deadline={}, deleted={}", deadline, localTotal);

        } catch (Exception e) {
            // 清理是运维动作，失败不能影响主业务，只记日志等下一轮
            log.error("历史数据清理任务异常", e);
        } finally {
            // 防止锁已易主时误删别人的锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
