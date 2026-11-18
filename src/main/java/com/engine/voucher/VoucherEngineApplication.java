package com.engine.voucher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.engine.voucher.mapper")
public class VoucherEngineApplication {
    public static void main(String[] args) {
        SpringApplication.run(VoucherEngineApplication.class, args);
    }
}