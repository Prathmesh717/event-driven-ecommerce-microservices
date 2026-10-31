package io.github.prathmesh717.analyticsservice.service;

import io.github.prathmesh717.analyticsservice.dto.MetricsResponse;

public interface AnalyticsService {

    void incrementTotalOrders();

    void incrementCompletedOrders();

    void incrementCancelledOrders();

    MetricsResponse getMetrics();
}