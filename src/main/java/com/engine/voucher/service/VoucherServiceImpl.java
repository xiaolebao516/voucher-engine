package com.engine.voucher.service;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.engine.voucher.constant.TraceConstant;
import com.engine.voucher.dto.RedisData;
import com.engine.voucher.entity.Voucher;
import com.engine.voucher.mapper.VoucherMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.MDC;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements VoucherService {

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private RBloomFilter<Long> bloomFilter;

    // 专门用来异步重建缓存的后台线程池（避免每次重建都临时创建销毁线程） 线程池用无界队列是大忌
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(4);
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(1);
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = new ThreadPoolExecutor(
            2,
            4,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(64), // 有界队列
            r -> new Thread(r, "voucher-cache-rebuild-" + THREAD_COUNTER.getAndIncrement()),
            new ThreadPoolExecutor.DiscardPolicy()
            // 默认丢弃策略，这里不能用callerRun是因为这样会让接受HTTP请求的tomcat去查数据库，接口响应时间会飙升
    );

    @PreDestroy
    public void destory() {
        CACHE_REBUILD_EXECUTOR.shutdown();
    }

    @PostConstruct
    public void init() {
        bloomFilter = redissonClient.getBloomFilter("voucher:bloom");
        bloomFilter.tryInit(10000L, 0.01);
    }

    // 仅用于上线前全量预热，查询复用这个会因为订单还未持久化写入旧值，避免用数据库旧库存覆盖 Redis 实时库存导致超卖
    @Override
    public void warmupVoucher(Long id) {
        Voucher voucher = getById(id);
        if (voucher == null) {
            return;
        }
        RedisData redisData = new RedisData();
        redisData.setData(voucher);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(30));
        stringRedisTemplate.opsForValue().set("voucher:detail:" + id, JSONUtil.toJsonStr(redisData));
        bloomFilter.add(id);
        stringRedisTemplate.opsForValue().set("seckill:stock:" + id, voucher.getStock().toString());
    }

    @Override
    public void rebuildVoucherDetailCache(Long id) {
        Voucher voucher = getById(id);
        if (voucher == null) {
            stringRedisTemplate.delete("voucher:detail:" + id);
        }
        // TODO:删缓存失败了怎么办
        RedisData redisData = new RedisData();
        redisData.setData(voucher);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(30));
        stringRedisTemplate.opsForValue().set("seckill:detail:" + id, JSONUtil.toJsonStr(redisData));
    }
    @Override
    public Voucher queryVoucherById(Long id) {
        // 布隆过滤器防穿透
        if (!bloomFilter.contains(id)) return null;
        // 从 Redis 查询缓存
        String json = stringRedisTemplate.opsForValue().get("voucher:detail:" + id);
        if (StrUtil.isBlank(json)) return null;
        // 反序列化并判断是否逻辑过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        Voucher voucher = JSONUtil.toBean((JSONObject) redisData.getData(), Voucher.class);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) return voucher;

        String traceId = MDC.get(TraceConstant.TRACE_ID_KEY);
        CACHE_REBUILD_EXECUTOR.submit(() -> {
            if (traceId != null) {
                MDC.put(TraceConstant.TRACE_ID_KEY, traceId);
            }
            RLock lock = redissonClient.getLock("lock:voucher:" + id);
            boolean isLocked = false;
            try {
                isLocked = lock.tryLock();
                if (!isLocked) {
                    return;
                }
                String newJson = stringRedisTemplate.opsForValue().get("voucher:detail:" + id);
                if (StrUtil.isNotBlank(newJson)) {
                    RedisData newRedisData = JSONUtil.toBean(newJson, RedisData.class);
                    if (newRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                        return;
                    }
                }
                rebuildVoucherDetailCache(id);
            } catch (Exception e) {
                log.error("异步重建优惠券详情缓存异常，voucherId={}", id, e);
            } finally {
                if (isLocked && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
                MDC.remove(TraceConstant.TRACE_ID_KEY);
            }
        });
        // 已逻辑过期，尝试获取互斥锁进行缓存重建
//        String lockKey = "lock:voucher:" + id;
//        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", 10, TimeUnit.SECONDS);
//
//        if (Boolean.TRUE.equals(isLock)) {
//            // 抢锁成功！提交异步任务给线程池去重建缓存（注意：主线程不等待！）
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    // 在后台线程里完成【双重检查 + 重建缓存】
//                    // 5.1 再查一次 Redis 拿到最新 json，反序列化出最新 RedisData，如果已经没过期了就直接 return;
//                    String newJson = stringRedisTemplate.opsForValue().get("voucher:detail:" + id);
//                    if (StrUtil.isNotBlank(newJson)) {
//                        RedisData newRedisData = JSONUtil.toBean(newJson, RedisData.class);
//                        if (newRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
//                            return;
//                        }
//                    }
//                    // 5.2 如果确实还过期，直接调用上面写好的 warmupVoucher(id) 查库并重写 Redis！
//                    warmupVoucher(id);
//
//                } finally {
//                    // 无论成功还是抛异常，最后必须释放互斥锁
//                    stringRedisTemplate.delete(lockKey);
//                }
//            });
//        }

        // 无论抢锁成功还是失败，主线程直接返回刚才解析出来的旧 voucher 对象
        return voucher; // 把这行改成 return voucher;
    }
}