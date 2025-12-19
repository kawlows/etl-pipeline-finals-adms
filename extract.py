import pandas as pd
import sqlite3
import os
import glob
from pathlib import Path


def load_csv_to_staging():
    """Extract CSV files from source and load into SQLite staging area."""
    try:
        # Create database connection
        db_path = "etl_database.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Define source paths
        japan_source = "data/source/japan_store"
        myanmar_source = "data/source/myanmar_store"

        # Check if source directories exist
        if not os.path.exists(japan_source):
            print(f"Warning: {japan_source} not found")
        if not os.path.exists(myanmar_source):
            print(f"Warning: {myanmar_source} not found")

        # Process Japan store files
        japan_files = glob.glob(f"{japan_source}/*.csv")
        if japan_files:
            print(f"Loading {len(japan_files)} file(s) from Japan store...")
            for csv_file in japan_files:
                try:
                    table_name = Path(csv_file).stem
                    df = pd.read_csv(csv_file)
                    print(f" -> {table_name} ({len(df)} rows)")
                    df.to_sql(f"japan_{table_name}", conn, if_exists="replace", index=False)
                except Exception as e:
                    print(f" ERROR loading {csv_file}: {e}")
        else:
            print(f"No CSV files found in {japan_source}")

        # Process Myanmar store files
        myanmar_files = glob.glob(f"{myanmar_source}/*.csv")
        if myanmar_files:
            print(f"Loading {len(myanmar_files)} file(s) from Myanmar store...")
            for csv_file in myanmar_files:
                try:
                    table_name = Path(csv_file).stem
                    df = pd.read_csv(csv_file)
                    print(f" -> {table_name} ({len(df)} rows)")
                    df.to_sql(f"myanmar_{table_name}", conn, if_exists="replace", index=False)
                except Exception as e:
                    print(f" ERROR loading {csv_file}: {e}")
        else:
            print(f"No CSV files found in {myanmar_source}")

        conn.commit()
        conn.close()
        print("\n✓ Staging complete!")
    except Exception as e:
        print(f"ERROR: {e}")
        raise


if __name__ == "__main__":
    load_csv_to_staging()
