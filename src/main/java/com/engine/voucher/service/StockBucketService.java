package com.engine.voucher.service;

import com.engine.voucher.entity.Voucher;
import com.engine.voucher.mapper.StockBucketMapper;
import com.engine.voucher.mapper.VoucherMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.function.BiFunction;

@Service
@RequiredArgsConstructor
public class StockBucketService {
    private final VoucherMapper voucherMapper;
    private final StockBucketMapper stockBucketMapper;
    private final TransactionTemplate transactionTemplate;

    /**
     * 库存模式按券持久化；只能在停流、无在途订单/消息时离线切换。
     * 每次尝试使用独立事务，且最多更新一条库存行。
     * 首选桶售罄时先完整回滚（包括订单、幂等记录和 outbox），再尝试下一桶，
     * 避免同一事务持有多个桶锁导致反向加锁死锁。仅库存不足触发换桶，
     * 其他异常仍由既有异常处理流程处理。
     */
    public <T> T execute(Long voucherId, Long userId, BiFunction<Integer, TransactionStatus, T> action) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("库存桶尝试必须从事务外开始");
        }
        Integer configured = voucherMapper.selectStockBucketCount(voucherId);
        int count = configured == null ? 0 : configured;
        if (count < 0 || count > 64) {
            throw new IllegalStateException("库存桶数量非法: " + count);
        }
        if (count == 0) {
            return transactionTemplate.execute(status -> action.apply(null, status));
        }
        int preferred = Math.floorMod(userId, count);
        for (int offset = 0; offset < count; offset++) {
            int bucket = (preferred + offset) % count;
            try {
                return transactionTemplate.execute(status -> action.apply(bucket, status));
            } catch (StockExhaustedException e) {
                // TransactionTemplate 已结束并回滚本次尝试，才允许访问下一桶。
            }
        }
        throw new StockExhaustedException();
    }

    public void deduct(Long voucherId, Integer bucketId) {
        int rows = bucketId == null ? voucherMapper.deductStock(voucherId)
                : stockBucketMapper.deduct(voucherId, bucketId);
        if (rows == 0) {
            throw new StockExhaustedException();
        }
    }

    public void refund(Long voucherId, Integer bucketId) {
        int rows = bucketId == null ? voucherMapper.refundStock(voucherId)
                : stockBucketMapper.refund(voucherId, bucketId);
        if (rows != 1) {
            throw new IllegalStateException("订单对应的库存行不存在: " + voucherId + "/" + bucketId);
        }
    }

    // 详情及预热使用真实库存总和；禁止逐单更新 tb_voucher.stock 造成新热点。
    public void applyStock(Voucher voucher) {
        Integer count = voucher.getStockBucketCount();
        if (count == null || count == 0) {
            return;
        }
        if (count < 0 || count > 64) {
            throw new IllegalStateException("库存桶数量非法: " + count);
        }
        Integer total = stockBucketMapper.total(voucher.getId(), count);
        if (total == null) {
            throw new IllegalStateException("库存桶初始化不完整: " + voucher.getId());
        }
        voucher.setStock(total);
    }

    public static class StockExhaustedException extends RuntimeException {
        public StockExhaustedException() {
            super("数据库库存扣减失败，库存不足");
        }
    }
}
