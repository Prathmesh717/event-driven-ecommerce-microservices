-- =====================================================
-- Database : analytics_db
-- Service  : analytics-service
-- =====================================================

-- CREATE DATABASE analytics_db;

CREATE TABLE IF NOT EXISTS order_metrics
(
    metric_name VARCHAR(100) PRIMARY KEY,

    metric_value BIGINT NOT NULL
);

-- =====================================================
-- Seed Metrics
-- =====================================================

INSERT INTO order_metrics
(
    metric_name,
    metric_value
)
VALUES
(
    'TOTAL_ORDERS',
    0
),
(
    'COMPLETED_ORDERS',
    0
),
(
    'CANCELLED_ORDERS',
    0
)
ON CONFLICT (metric_name)
DO NOTHING;