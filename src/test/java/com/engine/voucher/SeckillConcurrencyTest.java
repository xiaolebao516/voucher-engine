package com.engine.voucher;

import com.engine.voucher.service.VoucherOrderService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
public class SeckillConcurrencyTest {

    @Autowired
    private VoucherOrderService voucherOrderService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final Long VOUCHER_ID = 1L;
    private static final int INITIAL_STOCK = 100;

    @BeforeEach
    public void setUp() {
        jdbcTemplate.update("UPDATE tb_voucher SET stock = ? WHERE id = ?", INITIAL_STOCK, VOUCHER_ID);
        jdbcTemplate.execute("TRUNCATE TABLE tb_voucher_order");
        jdbcTemplate.execute("TRUNCATE TABLE tb_mq_consume_log");

        stringRedisTemplate.delete("seckill:order:" + VOUCHER_ID);
        stringRedisTemplate.opsForValue().set("seckill:stock:" + VOUCHER_ID, String.valueOf(INITIAL_STOCK));
    }

    @Test
    public void testConcurrentSeckillNoOversell() throws InterruptedException {
        int threadCount = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            long userId = 1000L + i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    String res = voucherOrderService.seckillByRedis(VOUCHER_ID, userId);
                    if (res != null && res.contains("抢券成功")) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executorService.shutdown();

        Thread.sleep(3000);

        String redisStock = stringRedisTemplate.opsForValue().get("seckill:stock:" + VOUCHER_ID);
        Integer dbStock = jdbcTemplate.queryForObject("SELECT stock FROM tb_voucher WHERE id = ?", Integer.class, VOUCHER_ID);
        Integer orderCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ?", Integer.class, VOUCHER_ID);

        Assertions.assertEquals(INITIAL_STOCK, successCount.get());
        Assertions.assertEquals(0, Integer.parseInt(redisStock));
        Assertions.assertEquals(0, dbStock);
        Assertions.assertEquals(INITIAL_STOCK, orderCount);
    }

    @Test
    public void testOneOrderPerUserConcurrency() throws InterruptedException {
        int threadCount = 50;
        long sameUserId = 9999L;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    String res = voucherOrderService.seckillByRedis(VOUCHER_ID, sameUserId);
                    if (res != null && res.contains("抢券成功")) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executorService.shutdown();

        Thread.sleep(2000);

        Integer orderCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id = ? AND user_id = ?",
                Integer.class, VOUCHER_ID, sameUserId
        );

        Assertions.assertEquals(1, successCount.get());
        Assertions.assertEquals(1, orderCount);
    }
}