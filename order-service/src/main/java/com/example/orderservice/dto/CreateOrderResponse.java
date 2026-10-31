package io.github.prathmesh717.orderservice.dto;

import java.util.UUID;

public record CreateOrderResponse(

        UUID orderId,

        String status
) {
}