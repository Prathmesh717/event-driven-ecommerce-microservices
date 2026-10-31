package io.github.prathmesh717.analyticsservice.dto;

public record MetricsResponse(

        Long totalOrders,

        Long completedOrders,

        Long cancelledOrders
) {
}