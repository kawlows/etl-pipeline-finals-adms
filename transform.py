import sqlite3
import os
import pandas as pd
from forex_python.converter import CurrencyRates


def clean_sqlite_table():
    """
    Clean and standardize all tables from Staging, save cleaned tables
    into the same DB and as CSVs in data/Transformation.
    """
    staging_db = os.path.join("data", "Staging", "etl_database.db")
    transform_folder = os.path.join("data", "Transformation")
    os.makedirs(transform_folder, exist_ok=True)

    conn = sqlite3.connect(staging_db)

    # FX: JPY -> USD (Japan), Myanmar already USD
    try:
        c = CurrencyRates()
        jpy_to_usd_rate = c.get_rate("JPY", "USD")
    except Exception:
        jpy_to_usd_rate = 1 / 150

    # ---- Items ----
    japan_items = pd.read_sql_query("SELECT * FROM japan_items", conn)
    if "price" in japan_items.columns:
        japan_items["price"] = japan_items["price"] * jpy_to_usd_rate
        japan_items["currency"] = "USD"
    japan_items.columns = [c.strip() for c in japan_items.columns]
    japan_items = japan_items.drop_duplicates()
    japan_items.to_sql("japan_items_clean", conn, if_exists="replace", index=False)
    japan_items.to_csv(os.path.join(transform_folder, "japan_items_clean.csv"), index=False)

    myanmar_items = pd.read_sql_query("SELECT * FROM myanmar_items", conn)
    if "price" in myanmar_items.columns:
        myanmar_items["currency"] = "USD"
    myanmar_items.columns = [c.strip() for c in myanmar_items.columns]
    myanmar_items = myanmar_items.drop_duplicates()
    myanmar_items.to_sql("myanmar_items_clean", conn, if_exists="replace", index=False)
    myanmar_items.to_csv(os.path.join(transform_folder, "myanmar_items_clean.csv"), index=False)

    # ---- Customers ----
    japan_customers = pd.read_sql_query("SELECT * FROM japan_customers", conn)
    japan_customers.columns = [c.strip() for c in japan_customers.columns]
    japan_customers = japan_customers.drop_duplicates()
    japan_customers.to_sql("japan_customers_clean", conn, if_exists="replace", index=False)
    japan_customers.to_csv(os.path.join(transform_folder, "japan_customers_clean.csv"), index=False)

    myanmar_customers = pd.read_sql_query("SELECT * FROM myanmar_customers", conn)
    myanmar_customers.columns = [c.strip() for c in myanmar_customers.columns]
    myanmar_customers = myanmar_customers.drop_duplicates()
    myanmar_customers.to_sql("myanmar_customers_clean", conn, if_exists="replace", index=False)
    myanmar_customers.to_csv(os.path.join(transform_folder, "myanmar_customers_clean.csv"), index=False)

    # ---- Branch ----
    japan_branch = pd.read_sql_query("SELECT * FROM japan_branch", conn)
    japan_branch.columns = [c.strip() for c in japan_branch.columns]
    japan_branch = japan_branch.drop_duplicates()
    japan_branch.to_sql("japan_branch_clean", conn, if_exists="replace", index=False)
    japan_branch.to_csv(os.path.join(transform_folder, "japan_branch_clean.csv"), index=False)

    myanmar_branch = pd.read_sql_query("SELECT * FROM myanmar_branch", conn)
    myanmar_branch.columns = [c.strip() for c in myanmar_branch.columns]
    myanmar_branch = myanmar_branch.drop_duplicates()
    myanmar_branch.to_sql("myanmar_branch_clean", conn, if_exists="replace", index=False)
    myanmar_branch.to_csv(os.path.join(transform_folder, "myanmar_branch_clean.csv"), index=False)

    # ---- Payment ----
    japan_payment = pd.read_sql_query("SELECT * FROM japan_payment", conn)
    japan_payment.columns = [c.strip() for c in japan_payment.columns]
    japan_payment = japan_payment.drop_duplicates()
    japan_payment.to_sql("japan_payment_clean", conn, if_exists="replace", index=False)
    japan_payment.to_csv(os.path.join(transform_folder, "japan_payment_clean.csv"), index=False)

    myanmar_payment = pd.read_sql_query("SELECT * FROM myanmar_payment", conn)
    myanmar_payment.columns = [c.strip() for c in myanmar_payment.columns]
    myanmar_payment = myanmar_payment.drop_duplicates()
    myanmar_payment.to_sql("myanmar_payment_clean", conn, if_exists="replace", index=False)
    myanmar_payment.to_csv(os.path.join(transform_folder, "myanmar_payment_clean.csv"), index=False)

    # ---- Sales ----
    japan_sales = pd.read_sql_query("SELECT * FROM japan_sales_data", conn)
    japan_sales.columns = [c.strip() for c in japan_sales.columns]
    if "id" in japan_sales.columns:
        japan_sales = japan_sales.dropna(subset=["id"])
    japan_sales["store"] = "Japan"
    japan_sales = japan_sales.drop_duplicates()
    japan_sales.to_sql("japan_sales_clean", conn, if_exists="replace", index=False)
    japan_sales.to_csv(os.path.join(transform_folder, "japan_sales_clean.csv"), index=False)

    myanmar_sales = pd.read_sql_query("SELECT * FROM myanmar_sales_data", conn)
    myanmar_sales.columns = [c.strip() for c in myanmar_sales.columns]
    if "id" in myanmar_sales.columns:
        myanmar_sales = myanmar_sales.dropna(subset=["id"])
    myanmar_sales["store"] = "Myanmar"
    myanmar_sales = myanmar_sales.drop_duplicates()
    myanmar_sales.to_sql("myanmar_sales_clean", conn, if_exists="replace", index=False)
    myanmar_sales.to_csv(os.path.join(transform_folder, "myanmar_sales_clean.csv"), index=False)

    conn.close()
    print("✓ Transform / clean complete")


if __name__ == "__main__":
    clean_sqlite_table()
