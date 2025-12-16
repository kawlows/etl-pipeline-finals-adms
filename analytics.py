import pandas as pd
import sqlite3

def generate_analytics():
    """
    Generate 5 key insights from the consolidated BIG_TABLE.
    """
    db_path = 'etl_database.db'
    conn = sqlite3.connect(db_path)
    
    # Read the consolidated table
    big_table = pd.read_sql_query("SELECT * FROM BIG_TABLE", conn)
    
    print("\n" + "="*70)
    print("ANALYTICS: 5 KEY INSIGHTS FROM ETL CONSOLIDATED DATA")
    print("="*70)
    
    # INSIGHT 1: Total Revenue by Store
    print("\n1. TOTAL REVENUE BY STORE (in USD):")
    print("-" * 70)
    revenue_by_store = big_table.groupby('store')['item_price_usd'].sum()
    for store, revenue in revenue_by_store.items():
        print(f"   {store}: ${revenue:,.2f}")
    total_revenue = revenue_by_store.sum()
    print(f"   TOTAL: ${total_revenue:,.2f}")
    
    # INSIGHT 2: Number of Transactions by Store
    print("\n2. TRANSACTION VOLUME BY STORE:")
    print("-" * 70)
    transactions_by_store = big_table.groupby('store')['transaction_id'].nunique()
    for store, count in transactions_by_store.items():
        print(f"   {store}: {count:,} transactions")
    print(f"   TOTAL: {transactions_by_store.sum():,} transactions")
    
    # INSIGHT 3: Average Transaction Value by Store
    print("\n3. AVERAGE TRANSACTION VALUE BY STORE (in USD):")
    print("-" * 70)
    avg_transaction_value = big_table.groupby('store')['item_price_usd'].mean()
    for store, avg_value in avg_transaction_value.items():
        print(f"   {store}: ${avg_value:,.2f}")
    print(f"   OVERALL AVERAGE: ${big_table['item_price_usd'].mean():,.2f}")
    
    # INSIGHT 4: Top 3 Items by Sales Volume
    print("\n4. TOP 3 BEST-SELLING ITEMS (by quantity):")
    print("-" * 70)
    top_items = big_table.groupby('item_name')['quantity'].sum().nlargest(3)
    for idx, (item, qty) in enumerate(top_items.items(), 1):
        print(f"   {idx}. {item}: {qty:,} units")
    
    # INSIGHT 5: Market Share Analysis
    print("\n5. MARKET SHARE ANALYSIS:")
    print("-" * 70)
    store_revenue = big_table.groupby('store')['item_price_usd'].sum()
    store_market_share = (store_revenue / store_revenue.sum() * 100)
    for store, share in store_market_share.items():
        print(f"   {store}: {share:.1f}% of total revenue")
    
    # Summary Statistics
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)
    print(f"Total Records in BIG_TABLE: {len(big_table):,}")
    print(f"Total Items Sold: {big_table['quantity'].sum():,} units")
    print(f"Average Items per Transaction: {big_table.groupby('transaction_id')['quantity'].sum().mean():.2f} units")
    print(f"Total Revenue: ${big_table['item_price_usd'].sum():,.2f}")
    print(f"Number of Unique Items: {big_table['item_name'].nunique()}")
    print(f"Number of Stores: {big_table['store'].nunique()}")
    print("="*70 + "\n")
    
    conn.close()

if __name__ == "__main__":
    generate_analytics()
