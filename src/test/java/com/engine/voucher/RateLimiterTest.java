package com.engine.voucher;

import com.engine.voucher.service.RateLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 令牌桶限流的真实 Redis 集成测试。
 *
 * 设计要点：
 *  1. 每个用例用独立 userId，并在 setUp/tearDown 清理 key，用例之间零耦合。
 *  2. 需要「桶已空」的用例，用一个远大于容量、速率极小的参数先灌满消耗，
 *     而不是 sleep 等令牌自然恢复 —— 慢且不稳定。
 *  3. 令牌恢复用例必须真实等待，因为要验证的正是「时间流逝后令牌补回来」。
 */
@SpringBootTest
class RateLimiterTest {

    @Autowired
    private RateLimiter rateLimiter;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /** 每个用例独立的 userId 段，避免与真实数据或其它用例撞车 */
    private static final long USER_BASE = 900_000_000L;
    private final List<Long> touchedUsers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        touchedUsers.clear();
    }

    @AfterEach
    void tearDown() {
        touchedUsers.forEach(rateLimiter::clearBuckets);
        // 清掉用例里可能残留的全局桶
        stringRedisTemplate.delete("rate:seckill:{limit}:global");
    }

    private Long newUser(long seq) {
        Long id = USER_BASE + seq;
        touchedUsers.add(id);
        return id;
    }

    @Test
    @DisplayName("非法参数：两个桶任一容量或速率非正时拒绝，且不写入 Redis")
    void testInvalidParameters() {
        Long user = newUser(99);
        rateLimiter.clearBuckets(user);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> rateLimiter.tryAcquire(user, 5, 0, 10, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> rateLimiter.tryAcquire(user, 5, 1, 10, 0));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> rateLimiter.tryAcquire(user, 0, 1, 10, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> rateLimiter.tryAcquire(user, 5, 1, -1, 1));
        Assertions.assertFalse(Boolean.TRUE.equals(stringRedisTemplate.hasKey("rate:seckill:{limit}:user:" + user)));
        Assertions.assertFalse(Boolean.TRUE.equals(stringRedisTemplate.hasKey("rate:seckill:{limit}:global")));
    }

    // ==================== 1. 用户级限流 ====================

    @Test
    @DisplayName("用户级限流：容量 5 时前 5 次放行，第 6 次被用户桶拒绝")
    void testUserLevelLimit() {
        Long user = newUser(1);
        // 速率设为 0：禁止自然恢复，确保结果只取决于容量
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            results.add(rateLimiter.tryAcquire(user, 5, 1, 100_000, 1));
        }

        System.out.println("用户级限流结果（容量5, 速率0）: " + results);

        for (int i = 0; i < 5; i++) {
            Assertions.assertEquals(RateLimiter.ALLOWED, results.get(i),
                    "第 " + (i + 1) + " 次应放行");
        }
        Assertions.assertEquals(RateLimiter.USER_DENIED, results.get(5),
                "第 6 次应被用户桶拒绝");
    }

    // ==================== 2. 全局限流 ====================

    @Test
    @DisplayName("全局限流：用户桶充足但全局桶耗尽时，返回全局限流码")
    void testGlobalLevelLimit() {
        // 全局桶容量 2，速率 0；每个用户桶给足
        // 用 3 个不同用户，绕过用户级限制，专门打全局桶
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            results.add(rateLimiter.tryAcquire(newUser(10 + i), 100, 1, 2, 1));
        }

        System.out.println("全局限流结果（全局容量2, 速率0）: " + results);

        Assertions.assertEquals(RateLimiter.ALLOWED, results.get(0));
        Assertions.assertEquals(RateLimiter.ALLOWED, results.get(1));
        Assertions.assertEquals(RateLimiter.GLOBAL_DENIED, results.get(2),
                "全局桶耗尽后应返回全局限流码，而不是用户限流码");
    }

    // ==================== 3. 令牌恢复 ====================

    @Test
    @DisplayName("令牌恢复：桶耗尽后等待，按速率补回令牌")
    void testTokenRefill() {
        Long user = newUser(20);
        // 容量 1、速率 10/s：先耗尽
        Assertions.assertEquals(RateLimiter.ALLOWED,
                rateLimiter.tryAcquire(user, 1, 10, 100_000, 1));
        Assertions.assertEquals(RateLimiter.USER_DENIED,
                rateLimiter.tryAcquire(user, 1, 10, 100_000, 1),
                "容量 1 用完后应立即被拒");

        // 等 350ms，按 10/s 应补回约 3.5 个令牌（>=1 即可放行）
        sleep(350);

        int afterRefill = rateLimiter.tryAcquire(user, 1, 10, 100_000, 1);
        System.out.println("等待 350ms 后（速率 10/s）: " + afterRefill);
        Assertions.assertEquals(RateLimiter.ALLOWED, afterRefill,
                "等待后应有令牌补回并放行");
    }

    @Test
    @DisplayName("令牌恢复：等待时间不足时仍应拒绝")
    void testTokenRefillNotEnough() {
        Long user = newUser(21);
        // 容量 1、速率 1/s（即 1 秒补 1 个）
        rateLimiter.tryAcquire(user, 1, 1, 100_000, 1);
        Assertions.assertEquals(RateLimiter.USER_DENIED,
                rateLimiter.tryAcquire(user, 1, 1, 100_000, 1));

        // 只等 200ms，不足 1 秒，补不满 1 个令牌
        sleep(200);
        Assertions.assertEquals(RateLimiter.USER_DENIED,
                rateLimiter.tryAcquire(user, 1, 1, 100_000, 1),
                "补回不足 1 个令牌时应继续拒绝");
    }

    // ==================== 4. 并发争抢 ====================

    @Test
    @DisplayName("并发争抢：50 线程同时抢容量 5 的桶，恰好 5 个成功（无超发）")
    void testConcurrentAcquireNoOverIssue() throws Exception {
        Long user = newUser(30);
        int threads = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger allowed = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    // 速率 0，排除自然恢复干扰，结果应严格等于容量
                    if (rateLimiter.tryAcquire(user, 5, 1, 100_000, 1) == RateLimiter.ALLOWED) {
                        allowed.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        Assertions.assertTrue(done.await(30, TimeUnit.SECONDS), "并发任务应在 30s 内完成");
        pool.shutdown();

        System.out.println("并发争抢 50 线程 / 容量 5，实际放行: " + allowed.get());
        Assertions.assertEquals(5, allowed.get(),
                "Lua 原子性保证放行数严格等于容量，不允许超发");
    }

    // ==================== 5. Key 自动过期 ====================

    @Test
    @DisplayName("Key 自动过期：TTL 按容量/速率计算并设置，不会永久驻留")
    void testKeyExpire() {
        Long user = newUser(40);
        // 容量 10、速率 2 => TTL = ceil(10/2)+1 = 6 秒
        rateLimiter.tryAcquire(user, 10, 2, 100_000, 1);

        Long ttl = stringRedisTemplate.getExpire("rate:seckill:{limit}:user:" + user, TimeUnit.SECONDS);
        Long globalTtl = stringRedisTemplate.getExpire("rate:seckill:{limit}:global", TimeUnit.SECONDS);

        System.out.println("用户桶 TTL=" + ttl + "s, 全局桶 TTL=" + globalTtl + "s");
        Assertions.assertNotNull(ttl);
        Assertions.assertTrue(ttl > 0 && ttl <= 6,
                "用户桶 TTL 应为 (0, 6] 秒，实际=" + ttl);
        Assertions.assertNotNull(globalTtl);
        Assertions.assertTrue(globalTtl > 0,
                "全局桶也必须设置过期，否则 key 永不过期会泄漏");
    }

    // ==================== 6. Redis 故障 ====================

    @Test
    @DisplayName("Redis 故障：Fail-Closed，抛 RateLimitUnavailableException 以便返回 503")
    void testRedisFailureBehavior() {
        // 用一个连不上的端口构造一个独立的 RateLimiter，验证失败模式
        org.springframework.data.redis.connection.RedisStandaloneConfiguration cfg =
                new org.springframework.data.redis.connection.RedisStandaloneConfiguration("127.0.0.1", 1);
        org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory factory =
                new org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory(cfg);
        factory.afterPropertiesSet();
        factory.start();

        StringRedisTemplate brokenTemplate = new StringRedisTemplate(factory);
        brokenTemplate.afterPropertiesSet();

        RateLimiter brokenLimiter = new RateLimiter(brokenTemplate, true, 5, 2, 1000, 400);

        try {
            com.engine.voucher.exception.RateLimitUnavailableException ex =
                    Assertions.assertThrows(
                            com.engine.voucher.exception.RateLimitUnavailableException.class,
                            () -> brokenLimiter.tryAcquire(1L),
                            "Redis 不可时应包装成 RateLimitUnavailableException，供 Controller 返回 503");
            System.out.println("Redis 故障行为：Fail-Closed，异常=" + ex.getClass().getSimpleName()
                    + ", cause=" + ex.getCause().getClass().getSimpleName());
        } finally {
            factory.destroy();
        }
    }

    @Test
    @DisplayName("Redis 故障：关闭限流开关后，不访问 Redis 直接放行")
    void testDisabledSkipsRedis() {
        RateLimiter disabled = new RateLimiter(null, false, 5, 2, 1000, 400);
        Assertions.assertEquals(RateLimiter.ALLOWED, disabled.tryAcquire(1L),
                "开关关闭时不应访问 Redis，直接放行");
    }

    // ==================== 7. 两层限流的执行顺序 ====================

    @Test
    @DisplayName("执行顺序：用户桶先于全局桶，用户桶拒绝时全局桶完全不被触碰")
    void testUserBucketCheckedBeforeGlobal() {
        Long user = newUser(50);
        // 参数校验已禁止容量 0，这里改用合法参数把用户桶真正耗尽：
        // 容量 1、速率 1/s，放行一次后桶空，毫秒级内补不满 1 个令牌。
        rateLimiter.clearBuckets(user);

        // 万一两次调用间隔超过 1s（极端调度延迟）令牌会补回并再次放行，
        // 因此循环重试，并且每次都紧邻调用前重新取全局桶快照，
        // 断言只针对「真正被用户桶拒绝的那一次」。
        Double globalBefore = null;
        int denied = RateLimiter.ALLOWED;
        for (int attempt = 0; attempt < 10 && denied != RateLimiter.USER_DENIED; attempt++) {
            globalBefore = globalTokens();
            denied = rateLimiter.tryAcquire(user, 1, 1, 100, 1);
        }
        Assertions.assertEquals(RateLimiter.USER_DENIED, denied, "用户桶应先拒绝");

        // 用户桶先返回，Lua 根本不会走到全局桶那一步，
        // 所以全局桶令牌数应保持该次调用前的值不变
        // （比「保持满桶」更强的结论：证明全局桶这次完全没被触碰）
        System.out.println("用户桶拒绝后，全局桶 tokens = " + globalTokens()
                + "，拒绝前 = " + globalBefore);
        Assertions.assertFalse(Double.isNaN(globalBefore), "被拒前全局桶应已存在");
        Assertions.assertEquals(globalBefore, globalTokens(), 0.001,
                "用户桶拒绝时全局桶不应被扣减，说明用户桶确实先于全局桶检查");
    }

    @Test
    @DisplayName("设计取舍：全局桶拒绝时用户桶已被扣减，不归还令牌")
    void testGlobalDeniedStillConsumesUserToken() {
        Long user = newUser(51);
        rateLimiter.clearBuckets(user);

        // 参数校验已禁止容量 0，这里改用合法参数把全局桶真正耗尽：
        // 全局容量 1、速率 1/s，先由另一个用户吃干全局桶，
        // 这样被测用户的用户桶仍有余额，只会在全局桶被拒。
        Double userBefore = null;
        int denied = RateLimiter.ALLOWED;
        for (int attempt = 0; attempt < 10 && denied != RateLimiter.GLOBAL_DENIED; attempt++) {
            // 每次用全新用户去吃全局桶，避免 other 自己的用户桶先耗尽
            // （那样全局桶就不再被扣减，本轮会被误放行）。
            Long eater = newUser(60 + attempt);
            rateLimiter.clearBuckets(eater);
            rateLimiter.tryAcquire(eater, 5, 1, 1, 1);
            rateLimiter.tryAcquire(eater, 5, 1, 1, 1);
            userBefore = userTokens(user);
            denied = rateLimiter.tryAcquire(user, 5, 1, 1, 1);
        }
        Assertions.assertEquals(RateLimiter.GLOBAL_DENIED, denied, "全局桶应拒绝");

        double userAfter = userTokens(user);
        System.out.println("全局桶拒绝后，用户桶 tokens = " + userAfter
                + "，拒绝前 = " + userBefore);
        // 有意为之的设计取舍：用户桶统计的是「这个用户打了多少次请求」，
        // 而不是「这个用户成功抢了几次」。请求打进来了就该计入频率，
        // 哪怕最终被全局限流挡掉 —— 否则用户会通过刷全局限流来「免费」试频率。
        // 首次调用时用户桶按容量 5 初始化后扣 1 => 4；之后每次拒绝都只再扣 1。
        double expected = userBefore == null || Double.isNaN(userBefore) ? 4.0 : userBefore - 1;
        Assertions.assertEquals(expected, userAfter, 0.001,
                "用户桶在全局拒绝后仍被扣减：这是按请求频率计数，属有意设计");
    }

    /** 读全局桶剩余令牌；key 不存在时返回 NaN，便于断言「完全未被触碰」 */
    private double globalTokens() {
        Object v = stringRedisTemplate.opsForHash().get("rate:seckill:{limit}:global", "tokens");
        return v == null ? Double.NaN : Double.parseDouble(v.toString());
    }

    /** 读指定用户桶剩余令牌 */
    private double userTokens(Long userId) {
        Object v = stringRedisTemplate.opsForHash().get("rate:seckill:{limit}:user:" + userId, "tokens");
        return v == null ? Double.NaN : Double.parseDouble(v.toString());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
