import os
from pathlib import Path

# Define output directory relative to this file
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"

def load_data(fact_payments, dim_customers):
    """
    Save processed DataFrames to CSV files in the output directory.

    Args:
        fact_payments (pd.DataFrame): Fact table for payments
        dim_customers (pd.DataFrame): Dimension table for customers
    """
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save fact table
    fact_payments.to_csv(OUTPUT_DIR / "fact_payments.csv", index=False)

    # Save dimension table
    dim_customers.to_csv(OUTPUT_DIR / "dim_customers.csv", index=False)
