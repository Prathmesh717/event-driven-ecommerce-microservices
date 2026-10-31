package io.github.prathmesh717.analyticsservice.repository;

import io.github.prathmesh717.analyticsservice.entity.OrderMetric;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderMetricRepository
        extends JpaRepository<OrderMetric, String> {
}