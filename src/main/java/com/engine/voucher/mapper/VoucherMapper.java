package com.engine.voucher.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.engine.voucher.entity.Voucher;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface VoucherMapper extends BaseMapper<Voucher> {

    // 扣库存用的是「条件更新 / CAS-on-stock」，不是基于 version 的乐观锁：
    // 判断"够不够"和"扣减"在同一条 SQL 里完成，靠 InnoDB 行锁保证原子性，
    // 影响行数为 0 即代表库存已抢光，所以不需要先查后改，也不会超卖。
    // 注意 version 只在这里自增、从未出现在 WHERE 条件里，它不参与任何并发控制。
    @Update("UPDATE tb_voucher SET stock = stock - 1, version = version + 1 WHERE id = #{id} AND stock > 0")
    int deductStock(@Param("id") Long id);

    @Update("UPDATE tb_voucher SET stock = stock + 1 WHERE id = #{id}")
    int refundStock(@Param("id") Long id);

}