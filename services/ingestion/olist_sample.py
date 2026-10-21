"""Synthetic data in the Olist CSV layout, for tests and for a demo without a Kaggle download.

Usage: python -m services.ingestion.olist_sample [DIR] [--months N] [--orders-per-month N]
Real Olist files dropped into the same directory work unchanged.
"""

import argparse
import csv
import random
from pathlib import Path

PAYMENT_TYPES = ("credit_card", "boleto", "voucher", "debit_card")
STATUSES = ("delivered", "delivered", "delivered", "shipped", "canceled")

COLUMNS = {
    "olist_orders_dataset.csv": [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_delivered_customer_date",
    ],
    "olist_order_items_dataset.csv": [
        "order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value",
    ],
    "olist_order_payments_dataset.csv": [
        "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value",
    ],
    "olist_customers_dataset.csv": [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
    ],
}


def write_sample(root: Path, months: int = 4, orders_per_month: int = 300, seed: int = 0) -> None:
    rng = random.Random(seed)
    tables: dict[str, list[dict]] = {name: [] for name in COLUMNS}
    for month in range(months):
        for n in range(orders_per_month):
            order_id = f"o{month:02d}{n:05d}"
            customer_id = f"c{month:02d}{n:05d}"
            status = rng.choice(STATUSES)
            day = rng.randint(1, 28)
            tables["olist_orders_dataset.csv"].append({
                "order_id": order_id,
                "customer_id": customer_id,
                "order_status": status,
                "order_purchase_timestamp": f"2018-{month + 1:02d}-{day:02d} 10:00:00",
                # as in the real data, undelivered orders have no delivery date
                "order_delivered_customer_date": (
                    f"2018-{month + 1:02d}-{min(day + 5, 28):02d} 12:00:00" if status == "delivered" else ""
                ),
            })
            tables["olist_customers_dataset.csv"].append({
                "customer_id": customer_id,
                "customer_unique_id": f"u{rng.randint(1, orders_per_month * months)}",
                "customer_zip_code_prefix": rng.randint(1000, 99999),
                "customer_city": rng.choice(("sao paulo", "rio de janeiro", "curitiba")),
                "customer_state": rng.choice(("SP", "RJ", "PR")),
            })
            total = 0.0
            for item in range(1, rng.randint(1, 3) + 1):
                price = round(rng.uniform(10, 300), 2)
                total += price
                tables["olist_order_items_dataset.csv"].append({
                    "order_id": order_id,
                    "order_item_id": item,
                    "product_id": f"p{rng.randint(1, 500)}",
                    "seller_id": f"s{rng.randint(1, 50)}",
                    "price": price,
                    "freight_value": round(rng.uniform(5, 40), 2),
                })
            tables["olist_order_payments_dataset.csv"].append({
                "order_id": order_id,
                "payment_sequential": 1,
                "payment_type": rng.choice(PAYMENT_TYPES),
                "payment_installments": rng.randint(1, 10),
                "payment_value": round(total, 2),
            })
    root.mkdir(parents=True, exist_ok=True)
    for name, rows in tables.items():
        with (root / name).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS[name])
            writer.writeheader()
            writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", type=Path, default=Path("data/olist"))
    parser.add_argument("--months", type=int, default=4)
    parser.add_argument("--orders-per-month", type=int, default=300)
    args = parser.parse_args()
    write_sample(args.root, args.months, args.orders_per_month)
    print(f"Sample Olist-format data written to {args.root}")


if __name__ == "__main__":
    main()
