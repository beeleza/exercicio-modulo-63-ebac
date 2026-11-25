package com.beeleza.order_service.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        List<OrderItem> items,
        BigDecimal total,
        Instant createdAt
) {
    public record OrderItem(String productId, int quantity, BigDecimal price) {}
}