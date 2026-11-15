"""Pull today's FX rates from the Frankfurter REST API (ECB data, no API key)."""
import json
import os

import requests

API_URL = "https://api.frankfurter.app/latest?from=EUR"


def extract_fx_rates(data_dir=None):
    data_dir = data_dir or os.getenv("DATA_DIR", "data")
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    rates = resp.json()["rates"]  # units of currency per 1 EUR
    rows = [{"currency": "EUR", "rate_to_eur": 1.0}]
    rows += [{"currency": cur, "rate_to_eur": 1 / rate} for cur, rate in rates.items()]
    path = os.path.join(data_dir, "fx_rates.json")
    with open(path, "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)
    return path


if __name__ == "__main__":
    print(extract_fx_rates())
