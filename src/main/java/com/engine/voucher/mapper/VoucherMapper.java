package com.engine.voucher.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.engine.voucher.entity.Voucher;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface VoucherMapper extends BaseMapper<Voucher> {

    // 核心乐观锁 SQL：利用 MySQL 行锁 + stock > 0 条件判断，绝不超卖
    @Update("UPDATE tb_voucher SET stock = stock - 1, version = version + 1 WHERE id = #{id} AND stock > 0")
    int deductStock(@Param("id") Long id);

    @Update("UPDATE tb_voucher SET stock = stock + 1 WHERE id = #{id}")
    int refundStock(@Param("id") Long id);

}