import pandas as pd
import matplotlib.pyplot as plt

print("--- Loading Cleaned Data ---")

# Load all 9 cleaned CSV files into dataframes
try:
    df_stocks = pd.read_csv("cleaned_stocks.csv")
    df_stores = pd.read_csv("cleaned_stores.csv")
    df_brands = pd.read_csv("cleaned_brands.csv")
    df_categories = pd.read_csv("cleaned_categories.csv")
    df_customers = pd.read_csv("cleaned_customers.csv")
    df_order_items = pd.read_csv("cleaned_order_items.csv")
    df_orders = pd.read_csv("cleaned_orders.csv")
    df_products = pd.read_csv("cleaned_products.csv")
    df_staffs = pd.read_csv("cleaned_staffs.csv")
    
    # Convert date columns back to datetime (CSV doesn't store types)
    df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
    df_orders['required_date'] = pd.to_datetime(df_orders['required_date'])
    df_orders['shipped_date'] = pd.to_datetime(df_orders['shipped_date'])

    print("All 9 cleaned files loaded successfully.")

except FileNotFoundError as e:
    print(f"Error: {e}. Make sure all 'cleaned_*.csv' files are in the same folder.")
    print("\n--- Starting Analysis ---")

# --- Use Case 1: Top-Selling Brands by Region and Store ---

# 1. Merge order_items with products (to get brand_id)
merged_df = pd.merge(df_order_items, df_products, on='product_id')

# 2. Merge with brands (to get brand_name)
merged_df = pd.merge(merged_df, df_brands, on='brand_id')

# 3. Merge with orders (to get store_id)
merged_df = pd.merge(merged_df, df_orders, on='order_id')

# 4. Merge with stores (to get store_name and state/region)
merged_df = pd.merge(merged_df, df_stores, on='store_id')

print("All merges for Use Case 1 completed.")

# 5. Group by region (state), store, and brand, then sum the total_price
brand_sales = merged_df.groupby(
    ['state', 'store_name', 'brand_name']
)['total_price'].sum().reset_index()

# 6. Sort the results to see the top brands
brand_sales_sorted = brand_sales.sort_values(
    by=['state', 'store_name', 'total_price'],
    ascending=[True, True, False]
)

# --- Show and Save the Result ---
print("\n--- Result: Top-Selling Brands by Region and Store ---")
print(brand_sales_sorted.to_string())

# Save this analysis to its own CSV file (a project deliverable)
brand_sales_sorted.to_csv("analysis_top_brands.csv", index=False)
print("\nSuccessfully saved analysis to 'analysis_top_brands.csv'")
# --- Use Case 2: Evaluate Staff Performance ---

# 1. Merge order_items with orders (to get staff_id for each sale)
staff_sales = pd.merge(df_order_items, df_orders, on='order_id')

# 2. Merge with staffs (to get staff names)
staff_sales = pd.merge(staff_sales, df_staffs, on='staff_id')

print("All merges for Use Case 2 completed.")

# 3. Group by staff details
staff_performance = staff_sales.groupby(
    ['staff_id', 'first_name', 'last_name']
).agg(
    total_sales=('total_price', 'sum'),      # Sum of all sales they handled
    total_orders=('order_id', 'nunique')   # Count of unique orders they handled
).reset_index()

# 4. Sort by total_sales to see top performers
staff_performance_sorted = staff_performance.sort_values(by='total_sales', ascending=False)

# --- Show and Save the Result ---
print("\n--- Result: Staff Performance by Total Sales ---")
print(staff_performance_sorted.to_string())

# Save this analysis to its own CSV file
staff_performance_sorted.to_csv("analysis_staff_performance.csv", index=False)
print("\nSuccessfully saved analysis to 'analysis_staff_performance.csv'")
# --- Use Case 3: Track Customer Order Fulfillment Status ---

# 1. Define the meaning of each status number
status_mapping = {
    1: 'Pending',
    2: 'Processing',
    3: 'Shipped',
    4: 'Delivered'
}

# 2. Create a new 'order_status_name' column
df_orders['order_status_name'] = df_orders['order_status'].map(status_mapping)

# 3. Count how many orders are in each status
status_summary = df_orders['order_status_name'].value_counts().reset_index()
status_summary.columns = ['order_status', 'order_count']

# --- Show and Save the Result ---
print("\n--- Result: Order Status Summary ---")
print(status_summary.to_string())
status_summary.to_csv("analysis_order_status_summary.csv", index=False)
print("Successfully saved analysis to 'analysis_order_status_summary.csv'")

# 4. Filter for *unfulfilled* orders (not yet delivered)
unfulfilled_orders = df_orders[df_orders['order_status'].isin([1, 2, 3])].sort_values(by='order_date')
unfulfilled_orders.to_csv("analysis_unfulfilled_orders.csv", index=False)
print(f"Found and saved {len(unfulfilled_orders)} unfulfilled orders to 'analysis_unfulfilled_orders.csv'")


# --- Use Case 7: Monitor Delayed Shipments ---

# 1. Filter for orders that have actually shipped
shipped_orders = df_orders[df_orders['shipped_date'].notnull()].copy()

# 2. Check for delays
shipped_orders['delayed'] = shipped_orders['shipped_date'] > shipped_orders['required_date']

# 3. Filter for *only* the delayed shipments
delayed_shipments = shipped_orders[shipped_orders['delayed'] == True]

# 4. Calculate how late they were
delayed_shipments['days_delayed'] = (delayed_shipments['shipped_date'] - delayed_shipments['required_date']).dt.days
delayed_shipments_report = delayed_shipments.sort_values(by='days_delayed', ascending=False)

# --- Show and Save the Result ---
print(f"\n--- Result: Delayed Shipments ---")
delay_percentage = (len(delayed_shipments) / len(shipped_orders)) * 100
print(f"Total Shipped Orders: {len(shipped_orders)}")
print(f"Delayed Shipments: {len(delayed_shipments)} ({delay_percentage:.2f}%)")

# Save this analysis to its own CSV file
delayed_shipments_report.to_csv("analysis_delayed_shipments.csv", index=False)
print("Successfully saved delayed shipments report to 'analysis_delayed_shipments.csv'")
# --- Use Case 4: Identify Most Profitable Product Categories ---

# 1. Merge order_items with products (to get category_id)
category_sales = pd.merge(df_order_items, df_products, on='product_id')

# 2. Merge with categories (to get category_name)
category_sales = pd.merge(category_sales, df_categories, on='category_id')

print("All merges for Use Case 4 completed.")

# 3. Group by category_name and sum the total_price
category_profitability = category_sales.groupby(
    'category_name'
)['total_price'].sum().reset_index()
# --- Use Case 4: Identify Most Profitable Product Categories ---

# 1. Merge order_items with products (to get category_id)
category_sales = pd.merge(df_order_items, df_products, on='product_id')

# 2. Merge with categories (to get category_name)
category_sales = pd.merge(category_sales, df_categories, on='category_id')

print("All merges for Use Case 4 completed.")

# 3. Group by category_name and sum the total_price
# --- THIS IS THE CORRECTED LINE ---
category_profitability = category_sales.groupby(
    'category_name'
).agg(
    total_sales=('total_price', 'sum')  # We explicitly name the new column 'total_sales'
).reset_index()

# 4. Sort by total_sales (which now exists)
category_profitability_sorted = category_profitability.sort_values(
    by='total_sales', # This line is now correct
    ascending=False
)

# --- Show and Save the Result ---
print("\n--- Result: Most Profitable Product Categories ---")
print(category_profitability_sorted.to_string())

# Save this analysis to its own CSV file
category_profitability_sorted.to_csv("analysis_category_profitability.csv", index=False)
print("\nSuccessfully saved analysis to 'analysis_category_profitability.csv'")
# --- Use Case 5: Analyze Stock Levels Across Stores ---

# 1. Merge stocks with products (to get product_name)
stock_levels = pd.merge(df_stocks, df_products, on='product_id')

# 2. Merge with stores (to get store_name)
stock_levels = pd.merge(stock_levels, df_stores, on='store_id')

print("All merges for Use Case 5 completed.")

# 3. Analysis A: Find OUT-OF-STOCK products (quantity = 0)
out_of_stock_products = stock_levels[stock_levels['quantity'] == 0]

# --- Show and Save Result A ---
print(f"\n--- Result: Out-of-Stock Products ---")
print(f"Found {len(out_of_stock_products)} product(s) that are out of stock.")
print(out_of_stock_products[['store_name', 'product_name', 'quantity']].to_string())
out_of_stock_products.to_csv("analysis_out_of_stock_products.csv", index=False)
print("Successfully saved out-of-stock report.")


# 4. Analysis B: Find LOW-STOCK products (quantity < 5 and > 0)
low_stock_threshold = 5
low_stock_products = stock_levels[
    (stock_levels['quantity'] < low_stock_threshold) &
    (stock_levels['quantity'] > 0)
].sort_values(by='quantity')

# --- Show and Save Result B ---
print(f"\n--- Result: Low-Stock Products (Less than {low_stock_threshold} units) ---")
print(f"Found {len(low_stock_products)} product(s) with low stock.")
print(low_stock_products[['store_name', 'product_name', 'quantity']].head(20).to_string()) # Show top 20
low_stock_products.to_csv("analysis_low_stock_products.csv", index=False)
print("Successfully saved low-stock report.")
# --- Use Case 6: Understand Order Trends Over Time ---

# 1. Merge orders with order_items to get sales data with dates
sales_trends_df = pd.merge(df_orders, df_order_items, on='order_id')

# 2. Set 'order_date' as the index for time-series analysis
# We only need the date and the price
sales_over_time = sales_trends_df[['order_date', 'total_price']].set_index('order_date')

print("Merges and indexing for Use Case 6 completed.")

# 3. Resample by Month
monthly_sales = sales_over_time.resample('M').agg(
    total_sales=('total_price', 'sum'),
    order_count=('total_price', 'count')
)

# 4. Resample by Week
weekly_sales = sales_over_time.resample('W').agg(
    total_sales=('total_price', 'sum'),
    order_count=('total_price', 'count')
)

# --- Show and Save the Trend Data ---
print("\n--- Result: Monthly Sales Trend (Top 5 rows) ---")
print(monthly_sales.head().to_string())
monthly_sales.to_csv("analysis_monthly_sales_trend.csv")
print("Successfully saved 'analysis_monthly_sales_trend.csv'")

print("\n--- Result: Weekly Sales Trend (Top 5 rows) ---")
print(weekly_sales.head().to_string())
weekly_sales.to_csv("analysis_weekly_sales_trend.csv")
print("Successfully saved 'analysis_weekly_sales_trend.csv'")


# --- Phase 4 (Partial): Create Visualizations for Trends ---
print("\nGenerating visualizations for sales trends...")

# Plot 1: Monthly Sales
plt.figure(figsize=(12, 6))
plt.plot(monthly_sales.index, monthly_sales['total_sales'], marker='o', linestyle='-')
plt.title('Monthly Sales Over Time')
plt.xlabel('Month')
plt.ylabel('Total Sales ($)')
plt.grid(True)
plt.tight_layout()
plt.savefig('plot_monthly_sales_trend.png')
print("Successfully saved 'plot_monthly_sales_trend.png'")

# Plot 2: Weekly Sales
plt.figure(figsize=(12, 6))
plt.plot(weekly_sales.index, weekly_sales['total_sales'], linestyle='-')
plt.title('Weekly Sales Over Time')
plt.xlabel('Week')
plt.ylabel('Total Sales ($)')
plt.grid(True)
plt.tight_layout()
plt.savefig('plot_weekly_sales_trend.png')
print("Successfully saved 'plot_weekly_sales_trend.png'")
# --- Use Case 7: Discover Customer Concentration and Demographics ---

# 1. Analysis A: Geographic Concentration by State
state_concentration = df_customers['state'].value_counts().reset_index()
state_concentration.columns = ['state', 'customer_count']

# --- Show and Save Result A ---
print("\n--- Result: Customer Concentration by State ---")
print(state_concentration.to_string())
state_concentration.to_csv("analysis_customer_concentration_state.csv", index=False)
print("Successfully saved state concentration report.")

# 2. Analysis B: Geographic Concentration by City (Top 10)
city_concentration = df_customers['city'].value_counts().reset_index()
city_concentration.columns = ['city', 'customer_count']

# --- Show and Save Result B ---
print("\n--- Result: Customer Concentration by City (Top 10) ---")
print(city_concentration.head(10).to_string())
city_concentration.to_csv("analysis_customer_concentration_city.csv", index=False)
print("Successfully saved city concentration report.")


# 3. Analysis C: Top Customers by Total Sales
# We already merged this in Use Case 6, but we'll do it again for clarity
customer_sales_df = pd.merge(df_orders, df_order_items, on='order_id')

# Group by customer_id to get total sales
customer_value = customer_sales_df.groupby('customer_id').agg(
    total_sales=('total_price', 'sum'),
    total_orders=('order_id', 'nunique')
).reset_index()

# Merge with customer details to get names
customer_value = pd.merge(customer_value, df_customers, on='customer_id')

# Sort to find top customers
top_customers = customer_value.sort_values(by='total_sales', ascending=False)

# --- Show and Save Result C ---
print("\n--- Result: Top 20 Customers by Sales ---")
print(top_customers[['first_name', 'last_name', 'city', 'state', 'total_sales', 'total_orders']].head(20).to_string())
top_customers.to_csv("analysis_top_customers.csv", index=False)
print("Successfully saved top customers report.")


# --- Phase 4 (Partial): Visualize Customer Concentration ---
print("\nGenerating visualization for customer demographics...")

plt.figure(figsize=(10, 6))
# Plotting the top 10 states for readability
top_10_states = state_concentration.head(10)
plt.bar(top_10_states['state'], top_10_states['customer_count'])
plt.title('Customer Concentration by State (Top 10)')
plt.xlabel('State')
plt.ylabel('Number of Customers')
plt.tight_layout()
plt.savefig('plot_customer_concentration_state.png')
print("Successfully saved 'plot_customer_concentration_state.png'")