package com.engine.voucher.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("tb_voucher_order")
public class VoucherOrder {
    // 订单ID不用数据库自增，由代码用雪花算法生成全局唯一ID
    @TableId(type = IdType.INPUT)
    private Long id;
    private Long userId;
    private Long voucherId;
    // null 表示旧版单行库存；否则记录实际扣减的桶（包括回退选中的桶）。
    private Integer stockBucketId;
    // 状态机：0-待核销 1-锁定中 2-已核销 3-已过期退还
    private Integer status;
    private LocalDateTime createTime;
    private LocalDateTime useTime;
    private LocalDateTime refundTime;
}