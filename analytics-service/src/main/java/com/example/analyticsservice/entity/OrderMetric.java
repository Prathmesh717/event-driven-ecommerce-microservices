package io.github.prathmesh717.analyticsservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "order_metrics")
public class OrderMetric {

    @Id
    @Column(name = "metric_name")
    private String metricName;

    @Column(name = "metric_value", nullable = false)
    private Long metricValue;
}