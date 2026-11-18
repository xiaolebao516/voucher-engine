package com.engine.voucher.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.engine.voucher.entity.VoucherOrder;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {
    // 状态机：0(待核销) -> 2(已核销)
    @Update("UPDATE tb_voucher_order SET status = 2, use_time = NOW() WHERE id = #{orderId} AND status = 0")
    int verifyOrder(@Param("orderId") Long orderId);

    // 状态机：0(待核销) -> 3(已过期退还)
    @Update("UPDATE tb_voucher_order SET status = 3, refund_time = NOW() WHERE id = #{orderId} AND status = 0")
    int timeoutOrder(@Param("orderId") Long orderId);
}