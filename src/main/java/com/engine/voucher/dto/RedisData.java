package com.engine.voucher.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class RedisData {
    private Object Data;
    private LocalDateTime expireTime;
}
