"""Generate deliberately dirty synthetic source files: orders.csv and customers.json."""
import csv
import json
import os
import random
from datetime import datetime, timedelta

DATA_DIR = os.getenv("DATA_DIR", "data")
CURRENCIES = ["EUR", "USD", "GBP", "CHF", "CAD"]
STATUSES = ["completed", "pending", "cancelled", "refunded"]
COUNTRIES = ["FR", "TN", "DE", "ES", "IT", "GB", "US", "CA", "CH"]
START = datetime(2024, 1, 1)


def generate(n_orders=None, n_customers=None, data_dir=DATA_DIR, seed=42):
    n_orders = n_orders or int(os.getenv("N_ORDERS", 1_000_000))
    n_customers = n_customers or int(os.getenv("N_CUSTOMERS", 100_000))
    orders_path = os.path.join(data_dir, "orders.csv")
    customers_path = os.path.join(data_dir, "customers.json")
    if os.path.exists(orders_path) and os.path.exists(customers_path):
        return orders_path, customers_path  # already generated
    os.makedirs(data_dir, exist_ok=True)
    rnd = random.Random(seed)

    with open(customers_path, "w") as f:
        for cid in range(1, n_customers + 1):
            f.write(json.dumps({
                "customer_id": cid,
                "name": f"  Customer {cid} " if cid % 50 == 0 else f"Customer {cid}",
                "country": rnd.choice(COUNTRIES).lower() if cid % 20 == 0 else rnd.choice(COUNTRIES),
                "signup_date": (START - timedelta(days=rnd.randint(0, 1500))).strftime("%Y-%m-%d"),
            }) + "\n")

    with open(orders_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "order_ts", "currency", "amount", "status"])
        for oid in range(1, n_orders + 1):
            r = rnd.random()
            # ~0.5% orphans (unknown customer), ~0.5% missing customer
            customer = n_customers + 1 + oid if r < 0.005 else "" if r < 0.01 else rnd.randint(1, n_customers)
            amount = round(rnd.uniform(1, 500), 2) * (-1 if rnd.random() < 0.005 else 1)
            status = rnd.choice(STATUSES)
            row = [
                oid, customer,
                (START + timedelta(seconds=rnd.randint(0, 700 * 86400))).strftime("%Y-%m-%d %H:%M:%S"),
                rnd.choice(CURRENCIES),
                amount,
                f" {status.upper()} " if rnd.random() < 0.02 else status,
            ]
            w.writerow(row)
            if rnd.random() < 0.01:  # ~1% exact duplicates
                w.writerow(row)
    return orders_path, customers_path


if __name__ == "__main__":
    print(generate())
