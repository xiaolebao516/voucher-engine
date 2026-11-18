package com.engine.voucher.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.engine.voucher.entity.VoucherOrder;
import org.springframework.transaction.annotation.Transactional;

public interface VoucherOrderService extends IService<VoucherOrder> {
    String seckillByDb(Long voucherId, Long userId);

    String seckillByRedis(Long voucherId, Long userId);

    String verifyOrder(Long orderId);

    @Transactional(rollbackFor = Exception.class)
    boolean refundOrderInDb(Long orderId, Long voucherId);
}