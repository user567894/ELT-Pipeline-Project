#NYC Taxi Project/scripts/download_data.py
import requests
import pandas as pd
from pathlib import Path

def download_taxi_data(year, month):
    """Download NYC taxi data for a specific year/month"""
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    
    output_path = Path(f"data/raw/yellow_tripdata_{year}_{month:02d}.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading {year}-{month:02d}...")
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"✓ Downloaded to {output_path}")
    else:
        print(f"Failed to download {year}-{month:02d}")

if __name__ == "__main__":
    # Download 3 months of data
    for month in range(1, 4):  # Jan, Feb, Mar 2024
        download_taxi_data(2024, month)