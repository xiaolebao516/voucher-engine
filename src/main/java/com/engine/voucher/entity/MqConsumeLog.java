package com.engine.voucher.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("tb_mq_consume_log")
public class MqConsumeLog {
    @TableId(type = IdType.INPUT)
    private String msgId;
    private String bizType; // 补上这一行！
    private LocalDateTime createTime;
}