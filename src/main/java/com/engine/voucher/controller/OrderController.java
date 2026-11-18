package com.engine.voucher.controller;

import com.engine.voucher.service.VoucherOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderController {

    private final VoucherOrderService voucherOrderService;

    // 券核销接口
    @PostMapping("/{orderId}/verify")
    public String verify(@PathVariable Long orderId) {
        return voucherOrderService.verifyOrder(orderId);
    }
}