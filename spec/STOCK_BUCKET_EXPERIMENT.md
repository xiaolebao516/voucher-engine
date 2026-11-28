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

## 消费者扩容（固定 16 桶，2026-11-28）

`bench/consumer_matrix.py --buckets 16 --requests 3000 --reps 3`（3000 请求/3000 库存、预热 500、限流关闭），每档 3 轮；Hikari 默认池 10，16 消费者受池限制后单独加 `--pool 20` 对照，其余变量不变。

- 纯排空 TPS（剔除注入期已落库）：4 消费者 201.5±0.7，8 消费者 231.1±4.3 为最优；16 消费者（池 10）183.7±31.9 反降，池 20 回升到 256.9±55.1 但波动大。
- 池 10 时 c=8 起 borrow 已非零（c=8 约 5.2k 次、c=16 约 8.8k 次），c=16 池等待 335s；JFR 栈显示约一半阻塞在 MQ 发布确认线程（确认回调里 `UPDATE tb_local_message.markSent` 另借连接），一半在消费线程。池 20 后 borrow 降到约 6.2k、70s。
- 池 20 的收益被行锁吃掉：同一桶行 `deduct_bucket` 均耗时 2.3→6.4ms、行锁等待 468→942、行锁时间 5.6s→17.5s，事务均值 15→24ms，MySQL 容器 CPU 22s→30s。16 桶 + 16 消费者下多个消费者争同一桶，DB 已到瓶颈（innodb_flush_log_at_trx_commit=1、sync_binlog=1）。
- 全部 15 轮一致：无超卖/重复/丢单，DB 与 Redis 库存归零，16 桶各 187~188 单，零换桶，无重试/死信/死锁日志。
- 结论：默认消费者取 8（纯排空最优且稳定）；16 消费者需同时扩大池，收益不稳定且把瓶颈转到行锁，不推荐。全局限流维持 global-capacity 1000 / global-rate 400：限流开启时成功 QPS 245~485、拒绝 50~61%、MQ 无积压；关闭后无保护，MQ ready 峰值可达 5000+，而本机排空能力只有约 200~260 TPS（16 桶、4~8 消费者；c=1 旧单库存行仅约 78~90 TPS），故 400/s 的放行速率与消费能力基本匹配。
