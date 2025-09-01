from Connection_sql import get_engine, read_query
from scd2_update import scd2_update

engine = get_engine("Transactions_DB")

# Load source snapshot
src_df = read_query(engine, "SELECT * FROM cleaned_customers")

# Load existing SCD2 table (if it exists)
try:
    dim_df = read_query(engine, "SELECT * FROM scd2_customers")
    if dim_df.empty:
        dim_df = None
except:
    dim_df = None  # first run

# Run SCD2 update
updated = scd2_update(src_df, dim_df,
                      natural_key="customer_id",
                      tracked_cols=["address", "phone_number", "loyalty_status"])

# Write back to MySQL
updated.to_sql("scd2_customers", con=engine, if_exists="replace", index=False)

print(updated.head())
