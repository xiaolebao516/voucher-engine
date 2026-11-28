# MySQL 库存分桶实验

基线 ed4a0c5：库存 UPDATE 已移到事务末尾。新方案仅拆 MySQL 库存行，Redis 仍是单总库存，订单唯一键仍全局有效。

- stock_bucket_count=0 继续使用原库存行，旧订单 stock_bucket_id=NULL。
- stock_bucket_count>0 表示实际桶数量；分桶券的权威库存是 SUM(tb_voucher_stock_bucket.stock)，不逐单更新 tb_voucher.stock。
- userId floorMod 选首桶，桶空则整个事务回滚后再试下一桶，成功订单记录真实桶；退款根据该字段退回原行。
- 桶配置不得在线改变。已有数据库先停止应用并备份，再手动执行 spec/migrations/001_stock_buckets.sql 一次；新库 init.sql 已包含结构。已有券不自动转换。没有未结束订单和在途消息时才可离线切换。禁止把线上或有业务数据的库用于 bench。
- 既有同步 DB 和异步 Redis 路径混跑时的 Redis/DB 总库存一致性、Redis 丢 Key 后的恢复以及消息可靠性边界仍与基线相同，未宣称解决。基准只测异步路径。

## 对照实验

同一个新 JAR，先审计并跑完整测试。仅隔离测试库，原脚本会清空测试订单与队列。每次运行指定固定桶数，其他设置相同：
```bash
python bench/consumer_matrix.py --consumers 1,2,4 --buckets 0 --reps 3 --requests 3000 --out-dir bench/results/buckets/legacy
python bench/consumer_matrix.py --consumers 1,2,4 --buckets 1 --reps 3 --requests 3000 --out-dir bench/results/buckets/1
python bench/consumer_matrix.py --consumers 1,2,4 --buckets 4 --reps 3 --requests 3000 --out-dir bench/results/buckets/4
python bench/consumer_matrix.py --consumers 1,2,4 --buckets 8 --reps 3 --requests 3000 --out-dir bench/results/buckets/8
python bench/consumer_matrix.py --consumers 1,2,4 --buckets 16 --reps 3 --requests 3000 --out-dir bench/results/buckets/16
```
先跑 smoke、验证迁移、回退桶和退款、跨路径并发、消费重投、订单/日志/outbox 回滚，再做正式压测。
记录真实落库数/全程耗时、实际积压区间的落库数增量/时间（旧 drain_tps 的分子含注入期间订单，不能当纯消费者 TPS）、桶扣减 SQL、行锁、错误/重试/死信、每桶剩余量、订单桶 ID。不要因性能结果忽略一致性。
