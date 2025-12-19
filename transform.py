import pandas as pd
import sqlite3
from forex_python.converter import CurrencyRates


def transform_data():
    """
    Clean and standardize data from staging tables.
    Convert JPY prices to USD for standardization.
    """
    db_path = "etl_database.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Get currency converter (mock rate if API unavailable: 1 USD = 150 JPY)
        try:
            c = CurrencyRates()
            jpy_to_usd_rate = c.get_rate("JPY", "USD")
        except Exception:
            # Fallback rate if API is unavailable
            jpy_to_usd_rate = 1 / 150  # ~0.0067

        # Process Japan items - convert prices from JPY to USD
        print("Transforming Japan items data...")
        japan_items = pd.read_sql_query("SELECT * FROM japan_japan_items", conn)
        if "price" in japan_items.columns:
            japan_items["price"] = japan_items["price"] * jpy_to_usd_rate
            japan_items["currency"] = "USD"
        japan_items.to_sql("japan_items_transformed", conn, if_exists="replace", index=False)

        # Process Myanmar items (already in USD, no conversion needed)
        print("Transforming Myanmar items data...")
        myanmar_items = pd.read_sql_query("SELECT * FROM myanmar_myanmar_items", conn)
        if "price" in myanmar_items.columns:
            myanmar_items["currency"] = "USD"
        myanmar_items.to_sql("myanmar_items_transformed", conn, if_exists="replace", index=False)

        # Clean and transform sales data
        print("Transforming Japan sales data...")
        japan_sales = pd.read_sql_query("SELECT * FROM japan_sales_data", conn)
        japan_sales = japan_sales.dropna(subset=["id"])
        japan_sales["store"] = "Japan"
        japan_sales.to_sql("japan_sales_transformed", conn, if_exists="replace", index=False)

        print("Transforming Myanmar sales data...")
        myanmar_sales = pd.read_sql_query("SELECT * FROM myanmar_sales_data", conn)
        myanmar_sales = myanmar_sales.dropna(subset=["id"])
        myanmar_sales["store"] = "Myanmar"
        myanmar_sales.to_sql("myanmar_sales_transformed", conn, if_exists="replace", index=False)

        conn.commit()
        print("Data transformation completed successfully!")
    finally:
        conn.close()


if __name__ == "__main__":
    transform_data()
