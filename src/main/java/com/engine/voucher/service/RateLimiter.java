package com.engine.voucher.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * 令牌桶限流：先扣用户桶，再扣全局桶。
 *
 * 返回值约定（Lua 与 Java 一致）：
 *   2 = 两层都通过，放行
 *   1 = 用户桶通过、全局桶拒绝
 *   0 = 用户桶拒绝
 */
@Component
public class RateLimiter {

    /** 允许通过 */
    public static final int ALLOWED = 2;
    /** 全局限流拒绝 */
    public static final int GLOBAL_DENIED = 1;
    /** 用户级限流拒绝 */
    public static final int USER_DENIED = 0;

    private static final DefaultRedisScript<Long> SCRIPT_TOKEN_BUCKET;

    static {
        SCRIPT_TOKEN_BUCKET = new DefaultRedisScript<>();
        SCRIPT_TOKEN_BUCKET.setLocation(new ClassPathResource("lua/token_bucket.lua"));
        SCRIPT_TOKEN_BUCKET.setResultType(Long.class);
    }

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 压测开关：关闭后 tryAcquire 直接返回放行，不再访问 Redis。
     * 用于「开启/关闭限流」两轮对照压测，避免为了对照组去改代码。
     */
    private final boolean enabled;
    private final int userCapacity;
    private final int userRate;
    private final int globalCapacity;
    private final int globalRate;

    public RateLimiter(StringRedisTemplate stringRedisTemplate,
                       @Value("${seckill.rate-limit.enabled:true}") boolean enabled,
                       @Value("${seckill.rate-limit.user-capacity:5}") int userCapacity,
                       @Value("${seckill.rate-limit.user-rate:2}") int userRate,
                       @Value("${seckill.rate-limit.global-capacity:1000}") int globalCapacity,
                       @Value("${seckill.rate-limit.global-rate:400}") int globalRate) {
        validateParameters(userCapacity, userRate, globalCapacity, globalRate);
        this.stringRedisTemplate = stringRedisTemplate;
        this.enabled = enabled;
        this.userCapacity = userCapacity;
        this.userRate = userRate;
        this.globalCapacity = globalCapacity;
        this.globalRate = globalRate;
    }

    /**
     * 生产路径：使用配置中的容量与速率。
     *
     * Redis 不可用时抛 {@link com.engine.voucher.exception.RateLimitUnavailableException}
     * （Fail-Closed）：抢券主链路同样依赖 Redis，限流器不能假装一切正常放行，
     * 否则 Redis 挂掉时会把所有流量灌进一个已经半死的主链路。
     */
    public int tryAcquire(Long userId) {
        if (!enabled) {
            return ALLOWED;
        }
        try {
            return tryAcquire(userId, userCapacity, userRate, globalCapacity, globalRate);
        } catch (Exception e) {
            throw new com.engine.voucher.exception.RateLimitUnavailableException(
                    "限流组件执行失败，Redis 可能不可用", e);
        }
    }

    /**
     * 可显式指定参数的版本，供测试使用。
     * 把参数显式化后，测试不必为了验证「容量 5 时第 6 次被拒」而去改配置文件。
     */
    public int tryAcquire(Long userId, int userCapacity, int userRate,
                          int globalCapacity, int globalRate) {
        validateParameters(userCapacity, userRate, globalCapacity, globalRate);
        Long result = stringRedisTemplate.execute(
                SCRIPT_TOKEN_BUCKET,
                Arrays.asList(
                        "rate:seckill:{limit}:user:" + userId,
                        "rate:seckill:{limit}:global"
                ),
                String.valueOf(userCapacity),
                String.valueOf(userRate),
                String.valueOf(globalCapacity),
                String.valueOf(globalRate)
        );

        if (result == null) {
            throw new IllegalStateException("限流脚本执行异常");
        }

        return result.intValue();
    }

    private static void validateParameters(int userCapacity, int userRate,
                                           int globalCapacity, int globalRate) {
        if (userCapacity <= 0 || userRate <= 0 || globalCapacity <= 0 || globalRate <= 0) {
            throw new IllegalArgumentException("令牌桶容量与补充速率必须为正数");
        }
    }

    /** 供测试清理 key，避免用例之间互相污染 */
    public void clearBuckets(Long userId) {
        stringRedisTemplate.delete(Arrays.asList(
                "rate:seckill:{limit}:user:" + userId,
                "rate:seckill:{limit}:global"
        ));
    }

    public boolean isEnabled() {
        return enabled;
    }
}
