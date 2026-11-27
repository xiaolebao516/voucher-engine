package com.engine.voucher.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.engine.voucher.entity.LocalMessage;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

@Mapper
public interface LocalMessageMapper extends BaseMapper<LocalMessage> {
    @Select("""
            SELECT * FROM tb_local_message
            WHERE STATUS = 0 AND next_retry_time <= #{now}
            ORDER BY next_retry_time ASC LIMIT #{limit}
    """)
    List<LocalMessage> selectPendingMessages(@Param("now") LocalDateTime now,
                                             @Param("limit") int limit);

    // markSent和markFailed都要做状态机+乐观锁，是为了防止MQ经典的超时不确定性（不确定broker是否收到）问题，防止扫表线程和afterCommit线程
    // 并发竞态时，投递超时异常准备markFailed，但其实扫表已经发了，这时不设status会将status为1的改为2
    @Update("""
            UPDATE tb_local_message
            SET status = 1
            WHERE id = #{id} AND status = 0
    """)
    int markSent(@Param("id") Long id);

    @Update("""
            UPDATE tb_local_message
            SET status = CASE WHEN retry_count + 1 >= #{maxRetryCount} THEN 2 ELSE 0 END,
                next_retry_time = CASE WHEN retry_count + 1 >= #{maxRetryCount} THEN next_retry_time ELSE #{nextRetryTime} END,
                retry_count = retry_count + 1
            WHERE id = #{id} AND status = 0
    """) // 如果超过最大重试次数，更新status为2，不再更新next retry time，
    int markFailed(@Param("maxRetryCount") int maxRetryCount,
                   @Param("nextRetryTime") LocalDateTime nextRetryTime,
                   @Param("id") Long id);

    /**
     * 按更新时间分批清理「已终结」的本地消息。
     *
     * 为什么 WHERE 里必须有 status IN (1, 2)，绝不能只按时间删：
     *   - status = 0 是「还没成功投递」的消息。删掉它就是彻底丢单，
     *     而且投递线程可能正拿着这行准备重试，删了会让重试静默失效。
     *   - status = 1 已投递成功、status = 2 重试耗尽已放弃，这两种才是终态，可以清。
     * 换句话说：时间只是「够不够老」，status 才是「能不能删」，两个条件缺一不可。
     *
     * 为什么用 update_time 而不是 create_time：一条消息可能卡在重试里好几天，
     * create_time 很老但它在 update_time 上刚刚才变成终态。按 update_time 删
     * 才能保证「成为终态后又过了 7 天」，不会误删刚放弃、还等着人看的记录。
     *
     * 为什么带 LIMIT：同 MqConsumeLogMapper.deleteByCreateTimeBefore，
     * 大批量 DELETE 会长时间持锁、撑大 undo log、拖慢主从同步。
     * 前提是 (status, update_time) 上有联合索引，见 init.sql 里的 idx_status_update_time。
     */
    @Delete("""
            DELETE FROM tb_local_message
            WHERE status IN (1, 2)
              AND update_time < #{deadline}
            LIMIT #{limit}
    """)
    int deleteFinishedBefore(@Param("deadline") LocalDateTime deadline,
                             @Param("limit") int limit);

    /**
     * 查询「重试耗尽被放弃」的消息，给告警和人工修复用。
     *
     * status = 2 意味着消息在本地表里躺到了上限次数，从未成功投递给 broker。
     * 这类消息在 RabbitMQ 里没有任何副本，死信队列也看不到它们 —— 只能查这张表。
     *
     * 为什么返回整行而不是 COUNT(*)：只知道「有 5 条坏了」没法修，
     * 必须拿到 msgId / bizId / payload 才能定位是哪几单、重发还是人工补单。
     *
     * 为什么带 since 参数：告警任务按 update_time 水位线增量扫，
     * 每次只取「上次扫完之后新变成 status=2」的那些，避免同一条消息反复告警。
     */
    @Select("""
            SELECT * FROM tb_local_message
            WHERE status = 2 AND update_time > #{since}
            ORDER BY update_time ASC
            LIMIT #{limit}
    """)
    List<LocalMessage> selectAbandonedSince(@Param("since") LocalDateTime since,
                                            @Param("limit") int limit);
}
