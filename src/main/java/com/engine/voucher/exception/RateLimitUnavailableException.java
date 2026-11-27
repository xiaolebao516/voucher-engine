package com.engine.voucher.exception;

/**
 * 限流组件本身不可用 —— Redis 连不上或脚本执行失败。
 *
 * 为什么单独一个异常类型：抢券主链路（Lua 扣减库存）同样依赖 Redis，
 * 所以这里采用 Fail-Closed（宁可拒绝也不放行），但必须和「正常限流」
 * 在 HTTP 层区分开：限流返回 429，故障返回 503。
 * 否则 Redis 挂掉时，监控看到满屏 429 会误判成「流量太大」，掩盖真实故障。
 */
public class RateLimitUnavailableException extends RuntimeException {

    public RateLimitUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
