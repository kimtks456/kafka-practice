package com.example.kafka.events.order;

import java.math.BigDecimal;

public record OrderItem(
    String productId,
    String productName,
    int quantity,
    BigDecimal unitPrice
) {}
