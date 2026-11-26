package com.engine.voucher.service;

import cn.hutool.core.util.IdUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.engine.voucher.config.RabbitMqConfig;
import com.engine.voucher.constant.TraceConstant;
import com.engine.voucher.entity.Voucher;
import com.engine.voucher.entity.VoucherOrder;
import com.engine.voucher.mapper.VoucherMapper;
import com.engine.voucher.mapper.VoucherOrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
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


    @Override
    @Transactional(rollbackFor = Exception.class) // 保证“扣库存”和“创建订单”要么同时成功，要么同时回滚
    public String seckillByDb(Long voucherId, Long userId) {
        // TODO 1：校验优惠券是否存在、状态是否为上架(status == 1)、当前时间是否在 beginTime 和 endTime 之间
        // 提示：用 voucherMapper.selectById(voucherId) 查出券，不满足条件直接 return 对应的失败提示字符串
        Voucher voucher = voucherMapper.selectById(voucherId);
        if (voucher == null) return "优惠券不存在";
        if (voucher.getStatus() != 1) return "优惠券未上架或已下架";
        if (LocalDateTime.now().isAfter(voucher.getEndTime()) || LocalDateTime.now().isBefore(voucher.getBeginTime()))
            return "不在抢购时间范围内";
        // TODO 2：校验“一人一单”（先在代码层拦一道，减少无效更新）
        // 提示：使用 MyBatis-Plus 提供的链式查询统计当前用户是否已买过该券：
         Long count = lambdaQuery().eq(VoucherOrder::getUserId, userId).eq(VoucherOrder::getVoucherId, voucherId).count();
        // 如果 count > 0，直接 return "每人限领一张，请勿重复抢购";
        if (count > 0) return "每人限领一张，请勿重复抢购";
        // TODO 3：调用 voucherMapper.deductStock(voucherId) 乐观锁扣减库存
        // 提示：该方法返回受影响的行数（int updated），如果 updated == 0，说明库存已被抢光，return "手慢了，优惠券已抢光";
        int updated = voucherMapper.deductStock(voucherId);
        if (updated == 0) return "手慢了，优惠券已抢光";
        // TODO 4：创建订单并写入 tb_voucher_order 表
        // 提示：new 一个 VoucherOrder，用 IdUtil.getSnowflakeNextId() 设置全局唯一 id，
        // 设置 userId、voucherId、status = 0、createTime = LocalDateTime.now()，
        VoucherOrder order = new VoucherOrder();
        order.setId(IdUtil.getSnowflakeNextId());
        order.setVoucherId(voucherId);
        order.setUserId(userId);
        order.setCreateTime(LocalDateTime.now());
        order.setStatus(0);
        // 然后用 try { save(order); } catch (DuplicateKeyException e) { ... } 捕获数据库唯一索引冲突（并发下防止同一个用户同时发两个请求绕过 TODO 2）
        // 如果捕获到 DuplicateKeyException，抛 RuntimeException 或直接返回提示（注意：为了让 @Transactional 回滚刚才扣掉的库存，并发撞车时应该抛出异常！）
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
        // TODO 1：执行 Lua 脚本
        // 提示：调用 stringRedisTemplate.execute(脚本对象, Keys列表, Args可变参数)
        // Keys列表用 Arrays.asList("seckill:stock:" + voucherId, "seckill:order:" + voucherId)
        // Args传 userId.toString()，返回值用 Long result 接收
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Arrays.asList("seckill:stock:" + voucherId, "seckill:order:" + voucherId),
                userId.toString());

        // TODO 2：根据 Lua 脚本的返回值判断结果
        // 提示：
        // 如果 result == null || result == 1L，返回 "手慢了，优惠券已抢光"
        // 如果 result == 2L，返回 "每人限领一张，请勿重复抢购"
        // 如果是 0L，返回 "抢券成功（已进入异步下单队列）"
//        if (result == null || result == 1L) return "手慢了，优惠券已抢光";
//        if (result == 2L) return "每人限领一张，请勿重复抢购";
//        if (result == 0L) return "抢券成功（已进入异步下单队列）";
//        return "系统异常";
        if (result == 1L) return "手慢了，优惠券已抢光";
        if (result == 2L) return "每人限领一张，请勿重复抢购";
        if (result == 0L) {
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
                        }
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