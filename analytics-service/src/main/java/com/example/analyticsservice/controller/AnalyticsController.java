package io.github.prathmesh717.analyticsservice.controller;

import io.github.prathmesh717.analyticsservice.dto.MetricsResponse;
import io.github.prathmesh717.analyticsservice.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    @GetMapping("/metrics")
    public MetricsResponse getMetrics() {

        return analyticsService.getMetrics();
    }
}