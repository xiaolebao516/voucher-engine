package com.engine.voucher.service;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.engine.voucher.dto.RedisData;
import com.engine.voucher.entity.Voucher;
import com.engine.voucher.mapper.VoucherMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements VoucherService {

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final StockBucketService stockBucketService;
    private RBloomFilter<Long> bloomFilter;

    // 记录当前正在排队或重建中的券 ID
    private final Set<Long> rebuildingIds = ConcurrentHashMap.newKeySet();
    // 注入 Spring 托管线程池（字段名与 Bean 名一致，不加 @Qualifier 避免 Lombok 构造器丢失注解）
    private final ThreadPoolTaskExecutor cacheRebuildExecutor;

    // 专门用来异步重建缓存的后台线程池（避免每次重建都临时创建销毁线程） 线程池用无界队列是大忌
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(4);
//    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(1);
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = new ThreadPoolExecutor(
//            2,
//            4,
//            60L,
//            TimeUnit.SECONDS,
//            new ArrayBlockingQueue<>(64), // 有界队列
//            r -> new Thread(r, "voucher-cache-rebuild-" + THREAD_COUNTER.getAndIncrement()),
//            new ThreadPoolExecutor.AbortPolicy()
//            // 默认丢弃策略，这里不能用callerRun是因为这样会让接受HTTP请求的tomcat去查数据库，接口响应时间会飙升
//    );


    @PostConstruct
    public void init() {
        bloomFilter = redissonClient.getBloomFilter("voucher:bloom");
        bloomFilter.tryInit(10000L, 0.01);
    }

    // 仅用于上线前全量预热，查询复用这个会因为订单还未持久化写入旧值，避免用数据库旧库存覆盖 Redis 实时库存导致超卖
    @Override
    public void warmupVoucher(Long id) {
        Voucher voucher = getById(id);
        if (voucher == null || !Integer.valueOf(1).equals(voucher.getStatus())) {
            return;
        }
        stockBucketService.applyStock(voucher);
        RedisData redisData = new RedisData();
        redisData.setData(voucher);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(30));
        stringRedisTemplate.opsForValue().set("voucher:detail:" + id, JSONUtil.toJsonStr(redisData));
        bloomFilter.add(id);
        stringRedisTemplate.opsForValue().setIfAbsent("seckill:stock:" + id, voucher.getStock().toString());
    }

    @Override
    // 供定时兜底任务调用：直接接收查好的对象省掉 N 次查库，用普通 set 强刷并补录布隆
    public void rebuildVoucherDetailCache(Voucher voucher) {
        if (voucher == null || voucher.getId() == null) {
            return;
        }
        stockBucketService.applyStock(voucher);
        RedisData redisData = new RedisData();
        redisData.setData(voucher);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(30));
        stringRedisTemplate.opsForValue().set("voucher:detail:" + voucher.getId(), JSONUtil.toJsonStr(redisData));
        // 顺手补录布隆过滤器，防止新上架券漏掉预热导致前端被误拦截
        bloomFilter.add(voucher.getId());
    }

    // 供异步线程池调用：只更新【仍存在】的缓存，防下架后死灰复燃
    @Override
    public void rebuildVoucherDetailCache(Long id) {
        Voucher voucher = getById(id);
        if (voucher == null || !Integer.valueOf(1).equals(voucher.getStatus())) {
            stringRedisTemplate.delete("voucher:detail:" + id);
            // 删key后，下架了查为Absent不能set覆盖，如果用空串防不住，我们query有对blank返回null兜底所以可以直接del
            return;
        }
        stockBucketService.applyStock(voucher);
        RedisData redisData = new RedisData();
        redisData.setData(voucher);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(30));
//        stringRedisTemplate.opsForValue().set("voucher:detail:" + id, JSONUtil.toJsonStr(redisData));
        // 这里换成Redis的SET XX，防止并发期间下架，旧缓存覆盖刚下架的key，原子拒绝写入，不能先get后set
        stringRedisTemplate.opsForValue().setIfPresent("voucher:detail:" + id, JSONUtil.toJsonStr(redisData));
        bloomFilter.add(id);
    }

    @Override
    public void rebuildWithRetryAndAlert(Long id) {
        int maxRetries = 3;
        for (int i = 1; i <= maxRetries; i++) {
            try {
                this.rebuildVoucherDetailCache(id);
                return;
            } catch (Exception e) {
                log.warn("异步缓存重建第{}次失败，voucherId={}", i, id, e);
                if (i == maxRetries) {
                    // 打出带固定前缀的 ERROR 日志方便 ELK/Prometheus 抓告警，预留 Webhook 钩子
                    log.error("[ALERT][CACHE_REBUILD_FAIL] 缓存重建连续失败，voucherId={}", id, e);
                    return;
                }
                try {
                    Thread.sleep(100L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
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

//        String traceId = MDC.get(TraceConstant.TRACE_ID_KEY);

        if (!rebuildingIds.add(id)) {
            return voucher;
        }
        try {
            cacheRebuildExecutor.submit(() -> {
                RLock lock = redissonClient.getLock("lock:voucher:" + id);
                boolean isLocked = false;
                try {
                    isLocked = lock.tryLock();
                    if (!isLocked) {
                        return;
                    }
                    String newJson = stringRedisTemplate.opsForValue().get("voucher:detail:" + id);
                    if(StrUtil.isBlank(newJson)) return;
                    RedisData newRedisData = JSONUtil.toBean(newJson, RedisData.class);
                    if (newRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                        return;
                    }
                    this.rebuildWithRetryAndAlert(id);
                } catch (Exception e) {
                    log.error("异步重建优惠券详情缓存异常，voucherId={}", id, e);
                } finally {
                    if (isLocked && lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                    rebuildingIds.remove(id);
                }
            });
        } catch (RejectedExecutionException e) {
            rebuildingIds.remove(id);
            log.warn("缓存重建线程池已满，放弃本次异步重建，voucherId={}", id);
        }
        return voucher;
//        if (!rebuildingIds.add(id)) {
//            return voucher; // 已有同款券任务在排队或执行，直接返回旧数据
//        }
//        try {
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                if (traceId != null) {
//                    MDC.put(TraceConstant.TRACE_ID_KEY, traceId);
//                }
//                // 分布式锁防多台集群机器同时查MySQL
//                RLock lock = redissonClient.getLock("lock:voucher:" + id);
//                boolean isLocked = false;
//                try {
//                    isLocked = lock.tryLock();
//                    if (!isLocked) {
//                        return;
//                    }
//                    String newJson = stringRedisTemplate.opsForValue().get("voucher:detail:" + id);
//                    if (StrUtil.isNotBlank(newJson)) {
//                        RedisData newRedisData = JSONUtil.toBean(newJson, RedisData.class);
//                        if (newRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
//                            return;
//                        }
//                    }
//                    this.rebuildWithRetryAndAlert(id);
//                } catch (Exception e) {
//                    log.error("异步重建优惠券详情缓存异常，voucherId={}", id, e);
//                } finally {
//                    if (isLocked && lock.isHeldByCurrentThread()) {
//                        lock.unlock();
//                    }
//                    MDC.remove(TraceConstant.TRACE_ID_KEY);
//                    rebuildingIds.remove(id); // 无论抢锁成败或重建异常，必须释放本地占位
//                }
//            });
//        } catch (Exception e) {
//            rebuildingIds.remove(id); // 队列满被拒绝时清理占位
//            log.warn("缓存重建线程池已满，放弃本次异步重建，voucherId={}", id);
        // 无论抢锁成功还是失败，主线程直接返回刚才解析出来的旧 voucher 对象
//        return voucher;

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
    }

    @Override
    public void offlineVoucher(Long id) {
        // 1. 先落库改状态（单条 SQL 自动提交，不要加 @Transactional 包住下面的 Redis 操作）
        boolean updated = lambdaUpdate()
                .eq(Voucher::getId, id)
                .set(Voucher::getStatus, 2)
                .update();
        if (!updated) {
            return;
        }

        // 2. 立刻删库存 Key，第一时间截断直接调秒杀接口的请求
        stringRedisTemplate.delete("seckill:stock:" + id);

        // 3. 尝试抢同一把重建锁，等正在跑的异步重建结束再删详情 Key（双保险）
        RLock lock = redissonClient.getLock("lock:voucher:" + id);
        boolean isLocked = false;
        try {
            isLocked = lock.tryLock(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 无论是否抢到锁，都必须删详情 Key；即便重建线程并发在跑，也会被 SET XX 原子挡住
            stringRedisTemplate.delete("voucher:detail:" + id);
            if (isLocked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}