# 项目契约：高并发分布式营销发券与核销引擎 (voucher-engine)

## 1. 技术栈
- JDK 17 + Spring Boot 3.2 + MyBatis-Plus
- MySQL 8.0 (3306) + Redis 7.0 (6379) + Redisson + RabbitMQ 3.13 (5672/15672)

## 2. 核心接口契约
1. GET  /api/voucher/{id}           -> 查询券详情（已完成：布隆过滤器+逻辑过期+异步重建）
2. POST /api/voucher/{id}/warmup    -> 库存预热到 Redis（已完成）
3. POST /api/voucher/{id}/seckill   -> 高并发抢券（第三关：DB乐观锁基准 -> Redis+Lua原子扣减）
4. POST /api/order/{orderId}/verify -> 券核销接口（第四关：状态机流转 0 -> 2，幂等防重）
5. [MQ死信消费者]                   -> 超时未核销自动退还库存（第四关：状态流转 0 -> 3）

## 3. 当前关卡进度
- [已完成] 第一关：Docker 启动三大中间件 + MySQL 建表初始化
- [已完成] 第二关：Spring Boot 骨架 + 多级缓存防线
- [已完成] 第三关：高并发扣减演进（DB乐观锁基准 -> Redis+Lua）
- [已完成] 第四关：RabbitMQ 异步削峰与状态机闭环
- [进行中] 第五关：JMeter 压测对比 + 面试题单