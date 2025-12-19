# etl-pipeline

Quick ETL (extract, transform, load) project for combining Japan and Myanmar store data.

## what it does

- **extract.py** - grabs CSV files from data/source and dumps them into SQLite
- **transform.py** - cleans stuff up, converts JPY prices to USD
- **load.py** - mashes the data together into one big table (BIG_TABLE)
- **analytics.py** - pulls 5 key insights from the final data

## setup

### install deps

```bash
pip install pandas sqlite3 forex-python
```

don't worry if forex-python fails to install - there's a fallback rate built in (150 JPY = 1 USD)

### folder structure

put your CSV files here:
```
data/source/japan_store/*.csv
data/source/myanmar_store/*.csv
```

some columns to expect:
- customer_id, item_id, quantity, price, date
- adjust the script if your columns are different

## running it

just run them in order:

```bash
python extract.py   # loads data into database
python transform.py # cleans and converts currency
python load.py      # creates the final table
python analytics.py # shows insights
```

## what you get

everything goes into `etl_database.db` (SQLite)

**main tables:**
- BIG_TABLE - all your data combined and ready to use
- analytics_sales_by_store - revenue summary by store

**5 insights from analytics.py:**
1. total revenue by store
2. transaction volume
3. average transaction value
4. top 3 items by quantity
5. market share %

## troubleshooting

- **no data loaded?** - make sure your CSV files are in the right folders
- **SQL errors?** - probably running out of order. start fresh with extract.py
- **forex API down?** - it'll use the 150 JPY/USD fallback automatically
- **weird numbers?** - double-check your source data, garbage in = garbage out

## notes

- the database gets recreated each run (old data overwritten)
- all prices end up in USD in the final table
- add more analysis by editing analytics.py
- this is student/project code - not production ready
