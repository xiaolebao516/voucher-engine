package com.engine.voucher.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface StockBucketMapper {
    @Update("UPDATE tb_voucher_stock_bucket SET stock = stock - 1 WHERE voucher_id = #{voucherId} AND bucket_id = #{bucketId} AND stock > 0")
    int deduct(@Param("voucherId") Long voucherId, @Param("bucketId") Integer bucketId);

    @Update("UPDATE tb_voucher_stock_bucket SET stock = stock + 1 WHERE voucher_id = #{voucherId} AND bucket_id = #{bucketId}")
    int refund(@Param("voucherId") Long voucherId, @Param("bucketId") Integer bucketId);

    @Select("SELECT CASE WHEN COUNT(*) = #{count} THEN COALESCE(SUM(stock), 0) ELSE NULL END FROM tb_voucher_stock_bucket WHERE voucher_id = #{voucherId}")
    Integer total(@Param("voucherId") Long voucherId, @Param("count") int count);
}
