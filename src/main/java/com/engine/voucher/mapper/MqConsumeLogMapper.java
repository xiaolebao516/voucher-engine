package com.engine.voucher.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.engine.voucher.entity.MqConsumeLog;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

@Mapper
public interface MqConsumeLogMapper extends BaseMapper<MqConsumeLog> {

    /**
     * 按创建时间分批清理幂等记录。
     *
     * 为什么必须带 LIMIT：不加 LIMIT 的 DELETE 可能一次锁几十万行，
     * 长时间占用 undo log 和行锁，导致主从延迟飙升、binlog 暴涨，
     * 甚至把同库其它业务的写入拖死。分批删 = 每批一个短事务，把压力摊平。
     *
     * 为什么用 create_time 而不是别的列：这张表没有 update，create_time 就是
     * 记录“这条消息是哪天消费的”的唯一依据，也是唯一能安全判断“够不够老”的列。
     * 前提是它上面有索引，否则每批都要全表扫描 —— 见 init.sql 里的 idx_create_time。
     */
    @Delete("""
            DELETE FROM tb_mq_consume_log
            WHERE create_time < #{deadline}
            LIMIT #{limit}
    """)
    int deleteByCreateTimeBefore(@Param("deadline") LocalDateTime deadline,
                                 @Param("limit") int limit);
}