"""
Filesystem Source - Generates daily CSV and Parquet files for Mar 3-9, 2026.
Simulates an additional sales channel (e-commerce / EDI exports).
"""

import os
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

CITIES = [
    ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Salvador", "BA"),
    ("Fortaleza", "CE"), ("Recife", "PE"), ("Manaus", "AM"), ("Brasília", "DF"),
    ("Goiânia", "GO"), ("Campo Grande", "MS"), ("Florianópolis", "SC"),
    ("Belém", "PA"), ("São Luís", "MA"), ("Maceió", "AL"), ("Vitória", "ES"),
    ("Campinas", "SP"), ("Santos", "SP"),
]

PRODUCTS = [
    (101, "Notebook Dell Inspiron 15",      "Eletrônicos",      3499.90),
    (102, "Smartphone Samsung Galaxy A54",  "Eletrônicos",      1799.90),
    (103, "Monitor LG 27\" 4K",             "Eletrônicos",      2199.90),
    (104, "Smart TV Samsung 55\" QLED",     "Eletrônicos",      4299.90),
    (105, "Tablet iPad Air 5",              "Eletrônicos",      4999.90),
    (106, "Teclado Mecânico Redragon",       "Periféricos",       399.90),
    (107, "Mouse Logitech MX Master 3",     "Periféricos",       599.90),
    (108, "Headset JBL Quantum 400",        "Áudio",             449.90),
    (109, "Cadeira Gamer ThunderX3",        "Móveis",           1299.90),
    (110, "Ar Condicionado Daikin 12k",     "Climatização",     3199.90),
]

SALESMEN = list(range(1, 21))

random.seed(42)

OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

START_DATE = date(2026, 3, 3)
END_DATE   = date(2026, 3, 9)


def generate_day(day: date) -> pd.DataFrame:
    is_weekend = day.weekday() >= 5
    n_records = random.randint(8, 12) if is_weekend else random.randint(20, 35)

    rows = []
    for _ in range(n_records):
        product = random.choice(PRODUCTS)
        city, state = random.choice(CITIES)
        salesman_id = random.choice(SALESMEN)
        quantity = random.randint(1, 6)
        rows.append({
            "source":       "filesystem",
            "sale_date":    day.isoformat(),
            "salesman_id":  salesman_id,
            "product_id":   product[0],
            "product_name": product[1],
            "category":     product[2],
            "city":         city,
            "state":        state,
            "quantity":     quantity,
            "unit_price":   product[3],
            "total_amount": round(quantity * product[3], 2),
        })
    return pd.DataFrame(rows)


def main():
    current = START_DATE
    while current <= END_DATE:
        df = generate_day(current)
        date_str = current.strftime("%Y%m%d")

        # CSV export
        csv_path = OUTPUT_DIR / f"sales_{date_str}.csv"
        df.to_csv(csv_path, index=False)
        print(f"[OK] CSV  → {csv_path}  ({len(df)} records)")

        # Parquet export (partitioned)
        parquet_path = OUTPUT_DIR / f"sales_{date_str}.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"[OK] Parquet → {parquet_path}  ({len(df)} records)")

        current += timedelta(days=1)

    print("\nAll files generated successfully.")


if __name__ == "__main__":
    main()
