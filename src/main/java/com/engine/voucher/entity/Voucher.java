package com.engine.voucher.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("tb_voucher")
public class Voucher {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String title;
    private Integer stock;
    private Integer status;
    private LocalDateTime beginTime;
    private LocalDateTime endTime;
    private Integer expireSeconds;
    private Integer version;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}