import pandas as pd
import os

print("Starting data cleaning process...")

# Define the names of your 9 original CSV files
file_names = [
    "stocks.csv", "stores.csv", "brands.csv", "categories.csv",
    "customers.csv", "order_items.csv", "orders.csv", "products.csv", "staffs.csv"
]

# Create a dictionary to hold all the data
dataframes = {}

# --- 1. Load All Files ---
for file in file_names:
    if os.path.exists(file):
        dataframes[file] = pd.read_csv(file)
        print(f"Successfully loaded {file}")
    else:
        print(f"Error: {file} not found in project folder.")

# --- 2. Perform Cleaning & Transformation ---

# a) Clean 'customers.csv'
# Fill 1,267 missing 'phone' numbers with 'N/A'
if "customers.csv" in dataframes:
    df = dataframes["customers.csv"]
    initial_nulls = df['phone'].isnull().sum()
    df['phone'] = df['phone'].fillna('N/A')
    print(f"[Cleaned customers.csv] Filled {initial_nulls} null values in 'phone' column.")
    dataframes["customers.csv"] = df

# b) Clean 'orders.csv'
# Convert date columns to datetime objects for correct analysis
if "orders.csv" in dataframes:
    df = dataframes["orders.csv"]
    date_cols = ['order_date', 'required_date', 'shipped_date']
    df[date_cols] = df[date_cols].apply(pd.to_datetime, errors='coerce')
    print(f"[Cleaned orders.csv] Converted {', '.join(date_cols)} to datetime.")
    dataframes["orders.csv"] = df

# c) Transform 'order_items.csv'
# Create the 'total_price' column for sales analysis
if "order_items.csv" in dataframes:
    df = dataframes["order_items.csv"]
    df['total_price'] = df['quantity'] * df['list_price'] * (1 - df['discount'])
    print(f"[Transformed order_items.csv] Created 'total_price' column.")
    dataframes["order_items.csv"] = df

print("\n--- 3. Save All Cleaned Files ---")

# Loop through all dataframes and save them with a 'cleaned_' prefix
for file_name, df in dataframes.items():
    cleaned_file_name = f"cleaned_{file_name}"
    df.to_csv(cleaned_file_name, index=False)
    print(f"Successfully saved {cleaned_file_name}")

print("\nData cleaning complete! All cleaned files are ready.")