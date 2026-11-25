package com.engine.voucher.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.engine.voucher.entity.LocalMessage;
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

}
