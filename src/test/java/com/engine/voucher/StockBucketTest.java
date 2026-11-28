package com.engine.voucher;

import com.engine.voucher.service.VoucherOrderService;
import com.engine.voucher.service.VoucherService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.*;

@SpringBootTest
class StockBucketTest {
    private static final long VOUCHER = 8300000001L;
    @Autowired JdbcTemplate jdbc;
    @Autowired VoucherOrderService orders;
    @Autowired VoucherService vouchers;
    @Autowired StringRedisTemplate redis;

    @BeforeEach
    void init() {
        jdbc.update("INSERT INTO tb_voucher(id,title,stock,stock_bucket_count,status,begin_time,end_time) VALUES (?, 'bucket-test',0,4,1,'2020-01-01','2099-12-31')", VOUCHER);
        for (int i = 0; i < 4; i++) {
            jdbc.update("INSERT INTO tb_voucher_stock_bucket(voucher_id,bucket_id,stock) VALUES (?,?,?)", VOUCHER, i, i == 0 ? 0 : 1);
        }
    }

    @AfterEach
    void cleanup() {
        jdbc.update("DELETE l FROM tb_local_message l JOIN tb_voucher_order o ON l.biz_id=CAST(o.id AS CHAR) WHERE o.voucher_id=?", VOUCHER);
        jdbc.update("DELETE m FROM tb_mq_consume_log m JOIN tb_voucher_order o ON m.msg_id=CAST(o.id AS CHAR) WHERE o.voucher_id=?", VOUCHER);
        jdbc.update("DELETE FROM tb_voucher_order WHERE voucher_id=?", VOUCHER);
        jdbc.update("DELETE FROM tb_voucher_stock_bucket WHERE voucher_id=?", VOUCHER);
        jdbc.update("DELETE FROM tb_voucher WHERE id=?", VOUCHER);
        redis.delete(java.util.List.of("seckill:stock:" + VOUCHER, "seckill:order:" + VOUCHER, "voucher:detail:" + VOUCHER));
    }

    private int total() {
        return jdbc.queryForObject("SELECT SUM(stock) FROM tb_voucher_stock_bucket WHERE voucher_id=?", Integer.class, VOUCHER);
    }

    @Test
    void fallbackAndRefundToActualBucket() {
        // 8000 % 4 = 0，首选桶已空，应转到桶 1。
        Assertions.assertEquals("抢券成功", orders.seckillByDb(VOUCHER, 8000L));
        Long orderId = jdbc.queryForObject("SELECT id FROM tb_voucher_order WHERE voucher_id=?", Long.class, VOUCHER);
        Assertions.assertEquals(1, jdbc.queryForObject("SELECT stock_bucket_id FROM tb_voucher_order WHERE id=?", Integer.class, orderId));
        Assertions.assertEquals(2, total());
        Assertions.assertTrue(orders.refundOrderInDb(orderId, VOUCHER));
        Assertions.assertFalse(orders.refundOrderInDb(orderId, VOUCHER));
        Assertions.assertEquals(3, total());
        Assertions.assertEquals(1, jdbc.queryForObject("SELECT stock FROM tb_voucher_stock_bucket WHERE voucher_id=? AND bucket_id=1", Integer.class, VOUCHER));
    }

    @Test
    void concurrentFallbackNoOversellOrGhostOrders() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        java.util.List<Future<String>> results = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) {
                long uid = 9000L + i * 4; // 所有请求首选空桶 0
                results.add(pool.submit(() -> { start.await(); return orders.seckillByDb(VOUCHER, uid); }));
            }
            start.countDown();
            int accepted = 0;
            for (Future<String> result : results) {
                String response = result.get(30, TimeUnit.SECONDS);
                if (response.contains("抢券成功")) accepted++;
                else Assertions.assertTrue(response.contains("抢光"), response);
            }
            Assertions.assertEquals(3, accepted);
            Assertions.assertEquals(0, total());
            Assertions.assertEquals(3, jdbc.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=?", Integer.class, VOUCHER));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void warmupUsesBucketSum() {
        vouchers.warmupVoucher(VOUCHER);
        Assertions.assertEquals("3", redis.opsForValue().get("seckill:stock:" + VOUCHER));
        Assertions.assertEquals(3, vouchers.queryVoucherById(VOUCHER).getStock());
        Assertions.assertEquals(0, jdbc.queryForObject("SELECT stock FROM tb_voucher WHERE id=?", Integer.class, VOUCHER));
    }
}
