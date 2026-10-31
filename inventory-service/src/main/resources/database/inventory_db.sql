-- =====================================================
-- Database : inventory_db
-- Service  : inventory-service
-- =====================================================

-- CREATE DATABASE inventory_db;

CREATE TABLE IF NOT EXISTS inventory
(
    product_id BIGINT PRIMARY KEY,

    available_quantity INTEGER NOT NULL,

    updated_at TIMESTAMP NOT NULL
);

-- =====================================================
-- Idempotency Support
-- Stores processed Kafka event ids to prevent
-- duplicate message processing.
-- =====================================================

CREATE TABLE IF NOT EXISTS processed_events
(
    event_id UUID PRIMARY KEY,

    processed_at TIMESTAMP NOT NULL
);

-- =====================================================
-- Seed Inventory Data
-- =====================================================

INSERT INTO inventory
(
    product_id,
    available_quantity,
    updated_at
)
VALUES
(
    1001,
    100,
    CURRENT_TIMESTAMP
),
(
    1002,
    50,
    CURRENT_TIMESTAMP
),
(
    1003,
    25,
    CURRENT_TIMESTAMP
),
(
    1004,
    10,
    CURRENT_TIMESTAMP
),
(
    1005,
    5,
    CURRENT_TIMESTAMP
),
(
    1006,
    1,
    CURRENT_TIMESTAMP
),
(
    1007,
    0,
    CURRENT_TIMESTAMP
);