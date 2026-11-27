package com.engine.voucher.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把限流相关的两类失败翻译成不同的 HTTP 状态码。
 *
 * 429 vs 503 的区别很重要：
 *   429 = 「你没资格，退避重试」—— 客户端行为，不是系统故障。
 *   503 = 「系统坏了，别重试」—— 运维信号，要触发告警。
 * 压测脚本按状态码分类统计，才能把「限流拒绝率」和「错误率」分开，
 * 不会把限流拒绝误算成成功吞吐。
 */
@Slf4j
@RestControllerAdvice
public class RateLimitExceptionHandler {

    @ExceptionHandler(RateLimitedException.class)
    public ResponseEntity<Map<String, Object>> handleRateLimited(RateLimitedException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", 429);
        body.put("message", e.getMessage());
        body.put("scope", e.getScope());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(body);
    }

    @ExceptionHandler(RateLimitUnavailableException.class)
    public ResponseEntity<Map<String, Object>> handleUnavailable(RateLimitUnavailableException e) {
        // 这是故障，必须留日志给告警系统
        log.error("[ALERT][RATE_LIMIT_UNAVAILABLE] 限流组件不可用，已按 Fail-Closed 拒绝请求", e);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", 503);
        body.put("message", "服务暂时不可用，请稍后重试");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
