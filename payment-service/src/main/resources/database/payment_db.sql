-- CREATE DATABASE payment_db;

CREATE TABLE IF NOT EXISTS payments
(
    id UUID PRIMARY KEY,

    order_id UUID NOT NULL,

    amount NUMERIC(19,2) NOT NULL,

    status VARCHAR(50) NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_payments_order_id
ON payments(order_id);
