import pandas as pd
import sqlite3
import os


def load_presentation():
    staging_db = os.path.join("data", "Staging", "etl_database.db")
    presentation_folder = os.path.join("data", "Presentation")
    os.makedirs(presentation_folder, exist_ok=True)

    conn = sqlite3.connect(staging_db)

    japan_items = pd.read_sql_query("SELECT * FROM japan_items_clean", conn)
    myanmar_items = pd.read_sql_query("SELECT * FROM myanmar_items_clean", conn)
    japan_sales = pd.read_sql_query("SELECT * FROM japan_sales_clean", conn)
    myanmar_sales = pd.read_sql_query("SELECT * FROM myanmar_sales_clean", conn)

    items = pd.concat([japan_items, myanmar_items], ignore_index=True)
    sales = pd.concat([japan_sales, myanmar_sales], ignore_index=True)

    # clean column names: remove extra quotes and spaces
    sales.columns = [c.strip().strip("'").strip('"') for c in sales.columns]
    items.columns = [c.strip().strip("'").strip('"') for c in items.columns]

    print("sales columns cleaned:", list(sales.columns))
    print("items columns cleaned:", list(items.columns))

    if "product_id" not in sales.columns:
        raise KeyError("product_id not found in sales")
    if "id" not in items.columns:
        raise KeyError("id not found in items")

    big_table = sales.merge(
        items,
        left_on="product_id",
        right_on="id",
        suffixes=("_sale", "_item")
    )

    big_table.to_sql("BIG_TABLE", conn, if_exists="replace", index=False)
    big_table.to_csv(os.path.join(presentation_folder, "BIG_TABLE.csv"), index=False)

    conn.close()
    print("✓ BIG_TABLE created")


if __name__ == "__main__":
    load_presentation()
