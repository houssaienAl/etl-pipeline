-- Runs once in the "warehouse" database on first container start.
CREATE DATABASE airflow;

-- No FK on fact_orders: Spark reloads with TRUNCATE; referential integrity is
-- enforced by the quality_checks task instead.
CREATE TABLE dim_customers (
    customer_id BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     CHAR(2),
    signup_date DATE
);

CREATE TABLE fact_orders (
    order_id    BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_ts    TIMESTAMP NOT NULL,
    currency    CHAR(3) NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    amount_eur  NUMERIC(12, 2) NOT NULL,
    status      TEXT NOT NULL
);
