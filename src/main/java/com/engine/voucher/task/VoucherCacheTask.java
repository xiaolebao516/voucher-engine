package com.engine.voucher.task;

import com.engine.voucher.entity.Voucher;
import com.engine.voucher.mapper.VoucherMapper;
import com.engine.voucher.service.VoucherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class VoucherCacheTask {

    private final VoucherMapper voucherMapper;
    private final VoucherService voucherService;
    private final RedissonClient redissonClient;
    private final StringRedisTemplate stringRedisTemplate;

    @Scheduled(fixedDelay = 300_000)
    public void refreshActiveVoucherCache() {
        RLock lock = redissonClient.getLock("lock:task:voucher:cache:refresh");
        if (!lock.tryLock()) {
            return;
        }
        try {
            // 查询状态为上架(status=1)的秒杀券
            List<Voucher> activeList = voucherMapper.selectActiveSeckillVouchers();
            for (Voucher voucher : activeList) {
                try {
                    // 强刷详情缓存 + 自动补录布隆过滤器，用重载的Voucher方法，避免1+N查库
                    voucherService.rebuildVoucherDetailCache(voucher);
                    // 库存兜底：严禁使用 set，必须用 setIfAbsent (SET NX) 仅在 Key 缺失时补齐
                    if (voucher.getStock() != null) {
                        stringRedisTemplate.opsForValue().setIfAbsent(
                                "seckill:stock:" + voucher.getId(),
                                voucher.getStock().toString()
                        );
                    }
                } catch (Exception e) {
                    log.error("定时校准优惠券缓存失败，voucherId={}", voucher.getId(), e);
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}