package com.example.order.domain;

import java.math.BigDecimal;
import java.util.List;

public record CreateOrderRequest(
    String customerId,
    List<OrderItemRequest> items
) {
    public record OrderItemRequest(
        String productId,
        String productName,
        int quantity,
        BigDecimal unitPrice
    ) {}
}
