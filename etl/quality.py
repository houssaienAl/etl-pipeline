"""Post-load data quality checks. Each query returns TRUE when the check FAILS."""
import os

import psycopg2

CHECKS = {
    "fact_orders is not empty": "SELECT COUNT(*) = 0 FROM fact_orders",
    "dim_customers is not empty": "SELECT COUNT(*) = 0 FROM dim_customers",
    "no orphan orders": """
        SELECT COUNT(*) > 0 FROM fact_orders o
        LEFT JOIN dim_customers c USING (customer_id) WHERE c.customer_id IS NULL""",
    "amounts are positive": "SELECT COUNT(*) > 0 FROM fact_orders WHERE amount <= 0 OR amount_eur <= 0",
    "statuses are valid": """
        SELECT COUNT(*) > 0 FROM fact_orders
        WHERE status NOT IN ('completed', 'pending', 'cancelled', 'refunded')""",
    "EUR orders are not converted": "SELECT COUNT(*) > 0 FROM fact_orders WHERE currency = 'EUR' AND amount <> amount_eur",
    "no orders in the future": "SELECT COUNT(*) > 0 FROM fact_orders WHERE order_ts > NOW() + INTERVAL '1 day'",
}


def run_checks():
    conn = psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        dbname=os.getenv("WAREHOUSE_DB", "warehouse"),
        user=os.getenv("WAREHOUSE_USER", "etl"),
        password=os.getenv("WAREHOUSE_PASSWORD", "etl"),
    )
    failed = []
    with conn, conn.cursor() as cur:
        for name, sql in CHECKS.items():
            cur.execute(sql)
            is_failing = cur.fetchone()[0]
            print(f"[{'FAIL' if is_failing else 'PASS'}] {name}")
            if is_failing:
                failed.append(name)
    conn.close()
    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")


if __name__ == "__main__":
    run_checks()
