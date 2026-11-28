package com.engine.voucher;

import com.engine.voucher.service.VoucherOrderService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 同步下单路径（seckillByDb）的事务边界。
 *
 * 背景：为了让同步路径与异步消费者（OrderCreateConsumer）的加锁顺序一致
 * （都是先 tb_voucher_order 唯一键、后 tb_voucher 热点行），把库存 UPDATE
 * 挪到了插入订单之后。这样「库存不足」不再是「还没写任何东西就返回」，
 * 而是「订单已插入、需要回滚」。如果漏了回滚，会提交出一个没扣库存的幽灵订单，
 * 库存和订单数从此对不上，且无法自愈。本测试锁死这个约束。
 */
@SpringBootTest
class SeckillByDbTest {

    @Autowired
    private VoucherOrderService voucherOrderService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final Long VOUCHER_ID = 1L;

    @BeforeEach
    void setUp() {
        // seckillByDb 要求 status=1 且在活动时间内，这里显式铺好，
        // 不依赖开发库里被手工改过的行（曾被下架成 status=0）。
        jdbcTemplate.update(
                "UPDATE tb_voucher SET stock = 0, status = 1, version = 0, "
                        + "begin_time = '2020-01-01 00:00:00', end_time = '2099-12-31 23:59:59' "
                        + "WHERE id = ?", VOUCHER_ID);
        jdbcTemplate.execute("TRUNCATE TABLE tb_voucher_order");
        stringRedisTemplate.delete("seckill:order:" + VOUCHER_ID);
    }

    @Test
    @DisplayName("库存不足：返回抢光文案，且订单必须整体回滚，不留幽灵订单")
    void testInsufficientStockRollsBackOrder() {
        String res = voucherOrderService.seckillByDb(VOUCHER_ID, 8001L);

        Assertions.assertTrue(res.contains("抢光"), "库存不足应返回抢光文案，实际=" + res);
        Integer orders = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ?", Integer.class, VOUCHER_ID);
        Assertions.assertEquals(0, orders, "库存不足时订单必须随事务回滚，不能留下没扣库存的订单");
        Integer stock = jdbcTemplate.queryForObject(
                "SELECT stock FROM tb_voucher WHERE id = ?", Integer.class, VOUCHER_ID);
        Assertions.assertEquals(0, stock, "库存不足时库存不应变成负数");
    }

    @Test
    @DisplayName("库存充足：订单与库存同时生效")
    void testSuccessPath() {
        jdbcTemplate.update("UPDATE tb_voucher SET stock = 3 WHERE id = ?", VOUCHER_ID);

        String res = voucherOrderService.seckillByDb(VOUCHER_ID, 8002L);

        Assertions.assertTrue(res.contains("抢券成功"), "库存充足应抢券成功，实际=" + res);
        Integer orders = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ?", Integer.class, VOUCHER_ID);
        Integer stock = jdbcTemplate.queryForObject(
                "SELECT stock FROM tb_voucher WHERE id = ?", Integer.class, VOUCHER_ID);
        Assertions.assertEquals(1, orders);
        Assertions.assertEquals(2, stock, "成功路径库存应恰好减 1");
    }

    @Test
    @DisplayName("并发扣最后一张：恰好一个成功，失败方的订单必须回滚")
    void testLastUnitRaceNoGhostOrder() throws InterruptedException {
        jdbcTemplate.update("UPDATE tb_voucher SET stock = 1 WHERE id = ?", VOUCHER_ID);

        int threads = 8;
        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(threads);
        java.util.concurrent.atomic.AtomicInteger success =
                new java.util.concurrent.atomic.AtomicInteger();

        for (int i = 0; i < threads; i++) {
            long userId = 8100L + i;   // 不同用户，绕开一人一单限制，专门争库存
            pool.submit(() -> {
                try {
                    start.await();
                    if (voucherOrderService.seckillByDb(VOUCHER_ID, userId).contains("抢券成功")) {
                        success.incrementAndGet();
                    }
                } catch (Exception ignored) {
                    // 唯一键/回滚相关异常都算失败
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        Assertions.assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));
        pool.shutdown();

        Integer orders = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ?", Integer.class, VOUCHER_ID);
        Integer stock = jdbcTemplate.queryForObject(
                "SELECT stock FROM tb_voucher WHERE id = ?", Integer.class, VOUCHER_ID);

        Assertions.assertEquals(1, success.get(), "只有一张库存，应恰好一个成功");
        Assertions.assertEquals(1, orders, "落库订单数必须等于成功数，失败方不能留订单");
        Assertions.assertEquals(0, stock);
    }
}
