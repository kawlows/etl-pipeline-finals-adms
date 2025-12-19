import pandas as pd
import sqlite3
import os


def load_csv():
    """
    Load all Japan & Myanmar CSVs from data/source into data/Staging/etl_database.db.
    """
    source_folder = os.path.join("data", "source")
    staging_folder = os.path.join("data", "Staging")
    os.makedirs(staging_folder, exist_ok=True)

    db_path = os.path.join(staging_folder, "etl_database.db")
    conn = sqlite3.connect(db_path)

    japan_folder = os.path.join(source_folder, "japan_store")
    myanmar_folder = os.path.join(source_folder, "myanmar_store")

    # ---- Japan files ----
    japan_files = {
        "japan_Customers.csv": "japan_customers",
        "japan_branch.csv": "japan_branch",
        "japan_items.csv": "japan_items",
        "japan_payment.csv": "japan_payment",
        "sales_data.csv": "japan_sales_data",
    }

    for fname, table in japan_files.items():
        path = os.path.join(japan_folder, fname)
        if os.path.exists(path):
            df = pd.read_csv(path)
            df.to_sql(table, conn, if_exists="replace", index=False)
            print(f"Loaded {table} from {path} ({len(df)} rows)")
        else:
            print("Missing:", path)

    # ---- Myanmar files ----
    myanmar_files = {
        "myanmar_customers.csv": "myanmar_customers",
        "myanmar_branch.csv": "myanmar_branch",
        "myanmar_items.csv": "myanmar_items",
        "myanmar_payment.csv": "myanmar_payment",
        "sales_data.csv": "myanmar_sales_data",
    }

    for fname, table in myanmar_files.items():
        path = os.path.join(myanmar_folder, fname)
        if os.path.exists(path):
            df = pd.read_csv(path)
            df.to_sql(table, conn, if_exists="replace", index=False)
            print(f"Loaded {table} from {path} ({len(df)} rows)")
        else:
            print("Missing:", path)

    conn.close()
    print("✓ Staging load complete")


if __name__ == "__main__":
    load_csv()
