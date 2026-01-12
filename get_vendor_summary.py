import os
import time
import logging
import sqlite3
import pandas as pd
from ingestion_db import ingest_db

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True
)

logging.info("Logging initialized successfully")

# -------------------------------------------------------------------
# Create Vendor Summary
# -------------------------------------------------------------------

def create_vendor_summary(conn):
    """
    This function merges multiple tables to generate a vendor-level
    sales and purchase summary.
    """
    logging.info("Running vendor summary SQL query")

    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price,
            pp.Volume
    ),

    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesDollars) / NULLIF(SUM(SalesQuantity), 0) AS AvgSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,

        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.AvgSalesPrice,
        ss.TotalExciseTax,

        fs.FreightCost

    FROM PurchaseSummary ps

    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
       AND ps.Brand = ss.Brand

    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber

    ORDER BY ps.TotalPurchaseDollars DESC;
    """, conn)

    logging.info("Vendor summary query executed successfully")
    logging.info("\n%s", vendor_sales_summary.head().to_string())

    return vendor_sales_summary


# -------------------------------------------------------------------
# Data Cleaning & Feature Engineering
# -------------------------------------------------------------------

def clean_data(df):
    """
    Cleans the vendor summary data and creates additional analytical columns.
    """
    logging.info("Cleaning vendor summary data")

    # Fill missing values
    df.fillna(0, inplace=True)

    # Trim string columns
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()

    # Derived metrics
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]

    df["ProfitMargin"] = (
        df["GrossProfit"] /
        df["TotalSalesDollars"].replace(0, pd.nan)
    ) * 100

    df["StockTurnover"] = (
        df["TotalSalesQuantity"] /
        df["TotalPurchaseQuantity"].replace(0, pd.nan)
    )

    df["SalesToPurchaseRatio"] = (
        df["TotalSalesDollars"] /
        df["TotalPurchaseDollars"].replace(0, pd.nan)
    )

    logging.info("Data cleaning completed")
    logging.info("\n%s", df.head().to_string())

    return df


# -------------------------------------------------------------------
# Main Execution
# -------------------------------------------------------------------

if __name__ == "__main__":
    try:
        start_time = time.time()
        logging.info("Starting vendor summary pipeline")


        # Database connection
        conn = sqlite3.connect("inventory.db")

        # Create summary
        summary_df = create_vendor_summary(conn)

        # Clean data
        clean_df = clean_data(summary_df)

        # Ingest data

        logging.info("Ingesting data into vendor_sales_summary table")
        ingest_db(clean_df, "vendor_sales_summary", conn)

        conn.commit()
        conn.close()

        elapsed = time.time() - start_time
        logging.info("Pipeline completed successfully in %.2f seconds", elapsed)

    except Exception as e:
        logging.exception("Pipeline failed due to an error")
        raise
