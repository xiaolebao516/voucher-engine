package com.engine.voucher.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfig {

    // 1. 正常异步下单队列（durable = true 开启持久化，断电不丢消息）
    public static final String ORDER_EXCHANGE = "order.direct.exchange";
    public static final String ORDER_CREATE_QUEUE = "order.create.queue";
    public static final String ORDER_CREATE_ROUTING_KEY = "order.create";

    // 2. 延迟队列（存待核销订单，过期后自动转入死信交换机）
    public static final String ORDER_DELAY_EXCHANGE = "order.delay.exchange";
    public static final String ORDER_DELAY_QUEUE = "order.delay.queue";
    public static final String ORDER_DELAY_ROUTING_KEY = "order.delay";

    // 3. 死信队列（接收超时未核销的订单，执行退库存）
    public static final String ORDER_DLX_EXCHANGE = "order.dlx.exchange";
    public static final String ORDER_TIMEOUT_DLQ = "order.timeout.dlq";
    public static final String ORDER_TIMEOUT_ROUTING_KEY = "order.timeout";

    @Bean
    public DirectExchange orderExchange() {
        return new DirectExchange(ORDER_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderCreateQueue() {
        return QueueBuilder.durable(ORDER_CREATE_QUEUE).build();
    }

    @Bean
    public Binding orderCreateBinding() {
        return BindingBuilder.bind(orderCreateQueue()).to(orderExchange()).with(ORDER_CREATE_ROUTING_KEY);
    }

    @Bean
    public DirectExchange orderDelayExchange() {
        return new DirectExchange(ORDER_DELAY_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderDelayQueue() {
        return QueueBuilder.durable(ORDER_DELAY_QUEUE)
                // 为了方便等会儿测试验收，这里先设 30 秒过期（生产环境改成 900000 即 15 分钟）
                .ttl(900000)
                .deadLetterExchange(ORDER_DLX_EXCHANGE)
                .deadLetterRoutingKey(ORDER_TIMEOUT_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding orderDelayBinding() {
        return BindingBuilder.bind(orderDelayQueue()).to(orderDelayExchange()).with(ORDER_DELAY_ROUTING_KEY);
    }

    @Bean
    public DirectExchange orderDlxExchange() {
        return new DirectExchange(ORDER_DLX_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderTimeoutDlq() {
        return QueueBuilder.durable(ORDER_TIMEOUT_DLQ).build();
    }

    @Bean
    public Binding orderTimeoutBinding() {
        return BindingBuilder.bind(orderTimeoutDlq()).to(orderDlxExchange()).with(ORDER_TIMEOUT_ROUTING_KEY);
    }
}