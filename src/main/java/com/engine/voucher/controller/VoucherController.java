package com.engine.voucher.controller;

import com.engine.voucher.entity.Voucher;
import com.engine.voucher.service.VoucherService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/voucher")
@RequiredArgsConstructor
public class VoucherController {

    private final VoucherService voucherService;

    @PostMapping("/{id}/warmup")
    public String warmup(@PathVariable Long id) {
        voucherService.warmupVoucher(id);
        return "预热成功";
    }

    @GetMapping("/{id}")
    public Voucher queryById(@PathVariable Long id) {
        return voucherService.queryVoucherById(id);
    }
}