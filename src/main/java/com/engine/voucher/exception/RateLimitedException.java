package com.engine.voucher.exception;

/**
 * 正常限流拒绝 —— 请求频率超过令牌桶容量。
 *
 * 与 {@link RateLimitUnavailableException} 的区别：
 *   本异常表示「系统正常工作，只是你请求太快了」，属于预期内的业务拒绝，
 *   返回 HTTP 429 Too Many Requests，调用方应退避重试。
 *   而 Redis 故障属于「系统坏了」，返回 503，语义完全不同 —— 混在一起
 *   会让监控分不清「被限流」和「服务不可用」。
 */
public class RateLimitedException extends RuntimeException {

    /** USER = 用户级限流（单用户请求过快）；GLOBAL = 全局限流（系统总闸门） */
    private final String scope;

    public RateLimitedException(String message, String scope) {
        super(message);
        this.scope = scope;
    }

    public String getScope() {
        return scope;
    }
}
