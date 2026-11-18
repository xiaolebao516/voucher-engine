package com.engine.voucher.controller;

import com.engine.voucher.service.VoucherOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/voucher")
@RequiredArgsConstructor
public class VoucherOrderController {

    private final VoucherOrderService voucherOrderService;

    // 基准接口：纯 MySQL 乐观锁抢券（第五关压测对比用）
    @PostMapping("/{id}/seckill-db")
    public String seckillByDb(@PathVariable("id") Long voucherId, @RequestParam Long userId) {
        return voucherOrderService.seckillByDb(voucherId, userId);
    }

    // 正式高并发接口：Redis + Lua 原子抢券
    @PostMapping("/{id}/seckill")
    public String seckill(@PathVariable("id") Long voucherId, @RequestParam Long userId) {
        return voucherOrderService.seckillByRedis(voucherId, userId);
    }
}