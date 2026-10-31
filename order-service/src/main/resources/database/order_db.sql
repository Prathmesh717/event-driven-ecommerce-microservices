-- =====================================================
-- Database : order_db
-- Service  : order-service
-- =====================================================

-- CREATE DATABASE order_db;

CREATE TABLE IF NOT EXISTS orders
(
    id UUID PRIMARY KEY,

    customer_id BIGINT NOT NULL,

    product_id BIGINT NOT NULL,

    quantity INTEGER NOT NULL,

    amount NUMERIC(19,2) NOT NULL,

    status VARCHAR(50) NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_orders_customer_id
    ON orders(customer_id);

CREATE INDEX idx_orders_product_id
    ON orders(product_id);

CREATE INDEX idx_orders_status
    ON orders(status);

CREATE INDEX idx_orders_created_at
    ON orders(created_at);


-- =====================================================
-- Outbox Events
-- =====================================================

CREATE TABLE IF NOT EXISTS outbox_events
(
    id UUID PRIMARY KEY,

    aggregate_id UUID NOT NULL,

    event_type VARCHAR(100) NOT NULL,

    payload TEXT NOT NULL,

    published BOOLEAN NOT NULL,

    created_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_outbox_events_published
    ON outbox_events(published);