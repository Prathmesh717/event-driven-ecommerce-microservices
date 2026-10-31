package io.github.prathmesh717.orderservice.service;

import io.github.prathmesh717.orderservice.dto.CreateOrderRequest;
import io.github.prathmesh717.orderservice.dto.CreateOrderResponse;

import java.util.UUID;

public interface OrderService {

    CreateOrderResponse createOrder(CreateOrderRequest request);

    void completeOrder(UUID orderId);

    void cancelOrder(UUID orderId);
}