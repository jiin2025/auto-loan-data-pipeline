import pandas as pd
from pathlib import Path

# Define the base directory relative to this file
BASE_DIR = Path(__file__).resolve().parent.parent

def extract_data():
    """
    Load source CSV files into pandas DataFrames.

    Returns:
        tuple: customers, loans, payments DataFrames
    """
    # Load customers data
    customers_path = BASE_DIR / "data" / "customers.csv"
    customers = pd.read_csv(customers_path)

    # Load loans data
    loans_path = BASE_DIR / "data" / "loans.csv"
    loans = pd.read_csv(loans_path)

    # Load payments data
    payments_path = BASE_DIR / "data" / "payments.csv"
    payments = pd.read_csv(payments_path)

    return customers, loans, payments
