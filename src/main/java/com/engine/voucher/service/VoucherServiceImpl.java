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
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements VoucherService {

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private RBloomFilter<Long> bloomFilter;

    // 专门用来异步重建缓存的后台线程池（避免每次重建都临时创建销毁线程）
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(4);

    @PostConstruct
    public void init() {
        bloomFilter = redissonClient.getBloomFilter("voucher:bloom");
        bloomFilter.tryInit(10000L, 0.01);
    }

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
    public Voucher queryVoucherById(Long id) {
        // TODO 1：布隆过滤器防穿透
        // 判断 !bloomFilter.contains(id)，如果不存在直接 return null;
        if (!bloomFilter.contains(id)) return null;
        // TODO 2：从 Redis 查询缓存
        // 用 stringRedisTemplate.opsForValue().get("voucher:detail:" + id) 拿到 json 字符串
        // 用 StrUtil.isBlank(json) 判断如果为空，直接 return null（未预热的券不允许直接打穿到库）
        String json = stringRedisTemplate.opsForValue().get("voucher:detail:" + id);
        if (StrUtil.isBlank(json)) return null;
        // TODO 3：反序列化并判断是否逻辑过期
        // 提示：
        // RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        // Voucher voucher = JSONUtil.toBean((JSONObject) redisData.getData(), Voucher.class);
        // 判断 redisData.getExpireTime().isAfter(LocalDateTime.now())，如果没过期，直接 return voucher;
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        Voucher voucher = JSONUtil.toBean((JSONObject) redisData.getData(), Voucher.class);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) return voucher;
        // TODO 4：已逻辑过期，尝试获取互斥锁进行缓存重建
        String lockKey = "lock:voucher:" + id;
        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", 10, TimeUnit.SECONDS);

        if (Boolean.TRUE.equals(isLock)) {
            // 抢锁成功！提交异步任务给线程池去重建缓存（注意：主线程不等待！）
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // TODO 5：在后台线程里完成【双重检查 + 重建缓存】
                    // 5.1 再查一次 Redis 拿到最新 json，反序列化出最新 RedisData，如果已经没过期了就直接 return;
                    String newJson = stringRedisTemplate.opsForValue().get("voucher:detail:" + id);
                    if (StrUtil.isNotBlank(newJson)) {
                        RedisData newRedisData = JSONUtil.toBean(newJson, RedisData.class);
                        if (newRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                            return;
                        }
                    }
                    // 5.2 如果确实还过期，直接调用上面写好的 warmupVoucher(id) 查库并重写 Redis！
                    warmupVoucher(id);

                } finally {
                    // 无论成功还是抛异常，最后必须释放互斥锁
                    stringRedisTemplate.delete(lockKey);
                }
            });
        }

        // TODO 6：无论抢锁成功还是失败，主线程直接返回刚才解析出来的旧 voucher 对象
        return voucher; // 把这行改成 return voucher;
    }
}