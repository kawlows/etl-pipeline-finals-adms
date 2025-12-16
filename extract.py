import pandas as pd
import sqlite3
import os
import glob
from pathlib import Path

def load_csv_to_staging():
    """
    Extract CSV files from source and load into SQLite staging area.
    """
    # Create database connection
    db_path = 'etl_database.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define source paths
    japan_source = 'data/source/japan_store'
    myanmar_source = 'data/source/myanmar_store'
    
    # Process Japan store files
    print("Loading Japan store data...")
    for csv_file in glob.glob(f"{japan_source}/*.csv"):
        table_name = Path(csv_file).stem  # Get filename without extension
        df = pd.read_csv(csv_file)
        print(f"Loading {table_name} from Japan store")
        df.to_sql(f"japan_{table_name}", conn, if_exists='replace', index=False)
    
    # Process Myanmar store files
    print("Loading Myanmar store data...")
    for csv_file in glob.glob(f"{myanmar_source}/*.csv"):
        table_name = Path(csv_file).stem  # Get filename without extension
        df = pd.read_csv(csv_file)
        print(f"Loading {table_name} from Myanmar store")
        df.to_sql(f"myanmar_{table_name}", conn, if_exists='replace', index=False)
    
    conn.commit()
    conn.close()
    print("Staging data loaded successfully!")

if __name__ == "__main__":
    load_csv_to_staging()
