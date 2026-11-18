package com.engine.voucher.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.engine.voucher.entity.Voucher;

public interface VoucherService extends IService<Voucher> {
    void warmupVoucher(Long id);
    Voucher queryVoucherById(Long id);
}