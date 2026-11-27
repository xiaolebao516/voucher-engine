package com.engine.voucher.service;

import cn.hutool.core.util.IdUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.constant.TraceConstant;
import com.engine.voucher.entity.Voucher;
import com.engine.voucher.entity.VoucherOrder;
import com.engine.voucher.exception.RateLimitedException;
import com.engine.voucher.mapper.VoucherMapper;
import com.engine.voucher.mapper.VoucherOrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements VoucherOrderService {
    // 类成员变量区域：提前加载 Lua 脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT_ROLLBACK;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("lua/seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        SECKILL_SCRIPT_ROLLBACK = new DefaultRedisScript<>();
        SECKILL_SCRIPT_ROLLBACK.setLocation(new ClassPathResource("lua/seckill_rollback.lua"));
        SECKILL_SCRIPT_ROLLBACK.setResultType(Long.class);
    }

    private final StringRedisTemplate stringRedisTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final VoucherOrderMapper voucherOrderMapper;
    private final VoucherMapper voucherMapper;
    private final RateLimiter rateLimiter;


    @Override
    @Transactional(rollbackFor = Exception.class) // 保证“扣库存”和“创建订单”要么同时成功，要么同时回滚
    public String seckillByDb(Long voucherId, Long userId) {
        // 校验优惠券是否存在、状态是否为上架(status == 1)、当前时间是否在 beginTime 和 endTime 之间
        Voucher voucher = voucherMapper.selectById(voucherId);
        if (voucher == null) return "优惠券不存在";
        if (!Integer.valueOf(1).equals(voucher.getStatus())) return "优惠券未上架或已下架";
        if (LocalDateTime.now().isAfter(voucher.getEndTime()) || LocalDateTime.now().isBefore(voucher.getBeginTime()))
            return "不在抢购时间范围内";
        // 校验“一人一单”（先在代码层拦一道，减少无效更新）
         Long count = lambdaQuery().eq(VoucherOrder::getUserId, userId).eq(VoucherOrder::getVoucherId, voucherId).count();
        // 如果 count > 0，直接 return "每人限领一张，请勿重复抢购";
        if (count > 0) return "每人限领一张，请勿重复抢购";
        // 调用 voucherMapper.deductStock(voucherId) 乐观锁扣减库存
        int updated = voucherMapper.deductStock(voucherId);
        if (updated == 0) return "手慢了，优惠券已抢光";
        // 创建订单并写入 tb_voucher_order 表
        VoucherOrder order = new VoucherOrder();
        order.setId(IdUtil.getSnowflakeNextId());
        order.setVoucherId(voucherId);
        order.setUserId(userId);
        order.setCreateTime(LocalDateTime.now());
        order.setStatus(0);
        try {
            save(order);
        }
        catch (DuplicateKeyException e) {
            throw new RuntimeException("每人限领一张，请勿重复抢购");
         }
        return "抢券成功";
    }

    @Override
    public String seckillByRedis(Long voucherId, Long userId) {
        // 令牌桶限流：先扣用户桶，再扣全局桶。
        // 正常限流抛 RateLimitedException(429)，Redis 故障抛 RateLimitUnavailableException(503)。
        int tokenResult = rateLimiter.tryAcquire(userId);
        if (tokenResult == RateLimiter.USER_DENIED) {
            throw new RateLimitedException("用户请求过多，请稍后重试", "USER");
        }
        if (tokenResult == RateLimiter.GLOBAL_DENIED) {
            throw new RateLimitedException("接口繁忙，请稍后重试", "GLOBAL");
        }

        // 继续执行库存预扣
        // 执行 Lua 脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Arrays.asList("seckill:stock:" + voucherId, "seckill:order:" + voucherId),
                userId.toString());

        // 根据 Lua 脚本的返回值判断结果
        if (Long.valueOf(1L).equals(result)) return "手慢了，优惠券已抢光";
        if (Long.valueOf(2L).equals(result)) return "每人限领一张，请勿重复抢购";
        if (Long.valueOf(0L).equals(result)) {
            // 1. 雪花算法生成全局唯一订单ID
            long orderId = IdUtil.getSnowflakeNextId();
            // 2. 组装消息体（用 JSON 字符串最通用，避免跨服务序列化兼容问题）
            Map<String, Object> msgMap = new HashMap<>();
            msgMap.put("orderId", orderId);
            msgMap.put("userId", userId);
            msgMap.put("voucherId", voucherId);
            String msgJson = JSONUtil.toJsonStr(msgMap);

            // 3. 发送到 RabbitMQ 异步下单队列
            try {
                String traceId = MDC.get(TraceConstant.TRACE_ID_KEY);
                rabbitTemplate.convertAndSend(
                        RabbitMqConfig.ORDER_EXCHANGE,
                        RabbitMqConfig.ORDER_CREATE_ROUTING_KEY,
                        msgJson,
                        message -> {
                            if (traceId != null) {
                                message.getMessageProperties().setHeader(TraceConstant.TRACE_ID_HEADER, traceId);
                            }
                            return message;
                        },
                        new CorrelationData("SECKILL:" + orderId + ":" + voucherId + ":" + userId)
                );
            }
            catch (Exception mqRx) {
                log.error("抢券消息投递mq失败，准备回滚Redis库存，orderId={}", orderId, mqRx);
                // srem userid + incr voucher stock lua
                try {
                    stringRedisTemplate.execute(SECKILL_SCRIPT_ROLLBACK,
                            Arrays.asList("seckill:stock:" + voucherId, "seckill:order:" + voucherId ),
                            userId.toString());
                } catch (Exception redisEx) {
                    log.error("MQ投递失败后，Redis回滚也异常，需要依赖定时对账修复，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId, redisEx);
                }
                return "系统繁忙，抢券结果待确认，请稍后查询";
            }

            return "抢券成功，订单号：" + orderId;
        }
        return "系统异常";
    }

    @Override
    public String verifyOrder(Long orderId) {
        int updated = voucherOrderMapper.verifyOrder(orderId);
        if (updated == 0) {
            return "核销失败：订单不存在、已核销或已超时过期";
        }
        return "核销成功";
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean refundOrderInDb(Long orderId, Long voucherId) {
        int updated = voucherOrderMapper.timeoutOrder(orderId);
        if (updated == 0) {
            return false; // 已经被核销或退过款了
        }
        voucherMapper.refundStock(voucherId);
        return true;
    }
}