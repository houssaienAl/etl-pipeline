"""Self-check for the cleaning rules. Run: python tests/test_transform.py"""
from datetime import datetime

from pyspark.sql import SparkSession

from etl.transform import CUSTOMERS_SCHEMA, FX_SCHEMA, ORDERS_SCHEMA, clean_customers, clean_orders


def test_clean_orders():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    ts = datetime(2024, 5, 1)
    customers = clean_customers(spark.createDataFrame(
        [(1, " Alice ", "fr", "2023-01-01"), (1, "Alice dup", "FR", "2023-01-01"), (2, "Bob", "US", "2023-02-01")],
        CUSTOMERS_SCHEMA,
    ))
    fx = spark.createDataFrame([("EUR", 1.0), ("USD", 0.5)], FX_SCHEMA)
    orders = spark.createDataFrame(
        [
            (1, 1, ts, "EUR", 10.0, "completed"),      # kept
            (1, 1, ts, "EUR", 10.0, "completed"),      # duplicate
            (2, 2, ts, "usd", 20.0, " COMPLETED "),    # kept, normalised, converted
            (3, 99, ts, "EUR", 10.0, "pending"),       # orphan customer
            (4, None, ts, "EUR", 10.0, "pending"),     # missing customer
            (5, 1, ts, "EUR", -5.0, "pending"),        # negative amount
            (6, 1, ts, "XXX", 5.0, "pending"),         # unknown currency
            (7, 1, ts, "EUR", 5.0, "bogus"),           # invalid status
        ],
        ORDERS_SCHEMA,
    )

    rows = {r.order_id: r for r in clean_orders(orders, customers, fx).collect()}
    assert set(rows) == {1, 2}, rows
    assert rows[2].currency == "USD" and rows[2].status == "completed"
    assert rows[2].amount_eur == 10.0 and rows[1].amount_eur == 10.0

    cust = {r.customer_id: r for r in customers.collect()}
    assert len(cust) == 2 and cust[2].country == "US" and cust[1].name.strip() == cust[1].name
    spark.stop()


if __name__ == "__main__":
    test_clean_orders()
    print("ok")
