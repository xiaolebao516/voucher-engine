package com.engine.voucher.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("tb_local_message")
public class LocalMessage {
    // 雪花id未设自增需要声明为INPUT
    @TableId(type = IdType.INPUT)
    private Long id;

    private String bizType;
    private String bizId;
    private String exchange;
    private String routingKey;
    private String payload;
    private String traceId;

    /** 0-待发送 1-已发送 2-发送失败放弃 */
    private Integer status;
    private Integer retryCount;
    private LocalDateTime nextRetryTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

}
