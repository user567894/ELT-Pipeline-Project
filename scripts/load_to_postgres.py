#NYC Taxi Project/scripts/load_to_postgres.py
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy import text 
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Database connection
engine = create_engine(DATABASE_URL)

def load_parquet_to_postgres(file_path, table_name="raw_taxi_trips"):
    """Load parquet file into Postgres"""
    print(f"Loading {file_path}...")
    
    # Read parquet file
    df = pd.read_parquet(file_path)
    
    # Basic data inspection
    print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # Load to Postgres (append mode for multiple files)
    df.to_sql(
        table_name, 
        engine, 
        if_exists='append', 
        index=False,
        chunksize=10000
    )
    print(f"Loaded {len(df)} rows to {table_name}") 

if __name__ == "__main__":
    raw_data_dir = Path("data/raw")
    
    # Load all parquet files
    for parquet_file in raw_data_dir.glob("*.parquet"):
        load_parquet_to_postgres(parquet_file)
    
    # Verify data loaded
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw_taxi_trips"))
        count = result.scalar()
        print(f"\nTotal rows in database: {count:,}")
