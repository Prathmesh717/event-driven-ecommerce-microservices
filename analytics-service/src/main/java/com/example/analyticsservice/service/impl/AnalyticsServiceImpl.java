package io.github.prathmesh717.analyticsservice.service.impl;

import io.github.prathmesh717.analyticsservice.dto.MetricsResponse;
import io.github.prathmesh717.analyticsservice.entity.OrderMetric;
import io.github.prathmesh717.analyticsservice.enums.MetricType;
import io.github.prathmesh717.analyticsservice.repository.OrderMetricRepository;
import io.github.prathmesh717.analyticsservice.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * ==========================================
 * Handles Analytics and Metrics
 * ==========================================
 *
 * Responsibilities:
 * - Track Total Orders
 * - Track Completed Orders
 * - Track Cancelled Orders
 * - Provide Metrics Dashboard Data
 *
 * Patterns Implemented:
 * - Event-Driven Analytics
 * - Eventual Consistency
 * - Database-per-Service Pattern
 *
 * This service maintains analytics data by
 * consuming business events published by
 * other microservices.
 *
 * Consumed Events:
 * - order-created
 * - inventory-reserved
 * - payment-refunded
 */
@Service
@RequiredArgsConstructor
public class AnalyticsServiceImpl
        implements AnalyticsService {

    private final OrderMetricRepository
            orderMetricRepository;

    /**
     * Increments the total number of orders.
     *
     * Triggered by:
     * order-created (Kafka Topic)
     */
    @Transactional
    @Override
    public void incrementTotalOrders() {

        incrementMetric(
                MetricType.TOTAL_ORDERS);
    }

    /**
     * Increments the number of successfully
     * completed orders.
     *
     * Triggered by:
     * inventory-reserved (Kafka Topic)
     */
    @Transactional
    @Override
    public void incrementCompletedOrders() {

        incrementMetric(
                MetricType.COMPLETED_ORDERS);
    }

    /**
     * Increments the number of cancelled orders.
     *
     * Triggered by:
     * payment-refunded (Kafka Topic)
     */
    @Transactional
    @Override
    public void incrementCancelledOrders() {

        incrementMetric(
                MetricType.CANCELLED_ORDERS);
    }

    /**
     * Returns aggregated analytics metrics.
     *
     * Used by:
     * GET /api/v1/analytics/metrics
     */
    @Override
    public MetricsResponse getMetrics() {

        return new MetricsResponse(
                getMetric(MetricType.TOTAL_ORDERS),
                getMetric(MetricType.COMPLETED_ORDERS),
                getMetric(MetricType.CANCELLED_ORDERS));
    }

    /**
     * Updates a metric by incrementing
     * its current value.
     */
    private void incrementMetric(
            MetricType metricType) {

        OrderMetric metric =
                orderMetricRepository.findById(
                                metricType.name())
                        .orElseThrow();

        metric.setMetricValue(
                metric.getMetricValue() + 1);

        orderMetricRepository.save(metric);
    }

    /**
     * Retrieves metric value.
     *
     * Returns 0 when metric record
     * does not exist.
     */
    private Long getMetric(
            MetricType metricType) {

        return orderMetricRepository.findById(
                        metricType.name())
                .map(OrderMetric::getMetricValue)
                .orElse(0L);
    }
}