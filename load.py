import pandas as pd
import sqlite3

def load_to_presentation():
    """
    Load consolidated data from transformation area to presentation area.
    Create a consolidated 'BIG_TABLE' combining Japan and Myanmar store data.
    """
    db_path = 'etl_database.db'
    conn = sqlite3.connect(db_path)
    
    print("Loading data to presentation area...")
    
    # Read transformed sales data
    japan_sales = pd.read_sql_query("SELECT * FROM japan_sales_transformed", conn)
    myanmar_sales = pd.read_sql_query("SELECT * FROM myanmar_sales_transformed", conn)
    
    # Read transformed items data
    japan_items = pd.read_sql_query("SELECT * FROM japan_items_transformed", conn)
    myanmar_items = pd.read_sql_query("SELECT * FROM myanmar_items_transformed", conn)
    
    # Combine sales data
    combined_sales = pd.concat([japan_sales, myanmar_sales], ignore_index=True)
    
    # Combine items data
    combined_items = pd.concat([japan_items, myanmar_items], ignore_index=True)
    
    # Create consolidated table (BIG_TABLE)
    # Merge sales with items information
    consolidated_table = pd.merge(
        combined_sales, 
        combined_items, 
        left_on='item_id',
        right_on='id',
        how='left'
    )
    
    # Select key columns and create final consolidated table
    final_table = consolidated_table[[
        'store', 'id_x', 'id_y', 'item_id', 'quantity',
        'name', 'price', 'currency'
    ]].copy()
    
    # Rename columns for clarity
    final_table.columns = ['store', 'transaction_id', 'item_key', 'item_id', 'quantity',
                           'item_name', 'item_price_usd', 'currency']
    
    # Save consolidated table
    final_table.to_sql('BIG_TABLE', conn, if_exists='replace', index=False)
    
    # Create analytics view - sales by store
    sales_by_store = final_table.groupby('store').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'item_price_usd': 'sum'
    }).rename(columns={
        'transaction_id': 'total_transactions',
        'quantity': 'total_items',
        'item_price_usd': 'total_revenue_usd'
    })
    
    sales_by_store.to_sql('analytics_sales_by_store', conn, if_exists='replace')
    
    conn.commit()
    conn.close()
    print("Consolidated BIG_TABLE created successfully!")
    print(f"Total records in BIG_TABLE: {len(final_table)}")

if __name__ == "__main__":
    load_to_presentation()
