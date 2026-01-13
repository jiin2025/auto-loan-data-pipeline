import pandas as pd

def transform_data(customers, loans, payments):
    """
    Transforms raw data into Fact and Dimension tables for analysis.
    """
    # 1. Convert date columns to datetime objects
    loans["start_date"] = pd.to_datetime(loans["start_date"])
    payments["payment_date"] = pd.to_datetime(payments["payment_date"])

    # 2. Data Integrity Check (Validation)
    assert customers.isnull().sum().sum() == 0, "Customers dataset contains null values"
    assert loans.isnull().sum().sum() == 0, "Loans dataset contains null values"

    # 3. Create Fact Table (Merging Payments with Loans)
    fact_payments = payments.merge(
        loans, on="loan_id", how="left"
    )

    # 4. Create Dimension Table
    dim_customers = customers.copy()

    return fact_payments, dim_customers
