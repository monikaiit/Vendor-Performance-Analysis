import pandas as pd
import os
import time
import logging
from sqlalchemy import create_engine

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine("sqlite:///inventory.db")

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

def load_raw_data():
    """Load CSV files from data/ directory into SQLite database."""
    start = time.time()

    for file in os.listdir("data"):
        if file.lower().endswith(".csv"):
            try:
                df = pd.read_csv(os.path.join("data", file))
                logging.info(f"Ingesting {file} into database")
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                logging.exception(f"Error ingesting {file}")

    end = time.time()
    total_time = (end - start) / 60

    logging.info("Ingestion complete")
    logging.info(f"Total time taken: {total_time:.2f} minutes")

if __name__ == "__main__":
    load_raw_data()
