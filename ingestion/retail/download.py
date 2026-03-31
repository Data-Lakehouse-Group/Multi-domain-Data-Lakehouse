"""
Instacart Online Grocery Basket Analysis Downloader
====================================================
Downloads the Instacart dataset from Kaggle using the Kaggle API
and saves CSV files to a local staging directory before Bronze ingestion.
 
Authentication:
    Place your kaggle.json in ~/.kaggle/kaggle.json (Linux/Mac)
    or C:/Users/<username>/.kaggle/kaggle.json (Windows)
    Download it from: https://www.kaggle.com/settings -> API -> Create New Token

    Authentication is also possible through environment variables:
    export KAGGLE_USERNAME=your_username
    export KAGGLE_KEY=your_key
 
Dataset: https://www.kaggle.com/competitions/instacart-market-basket-analysis
 
Usage:
    python ingestion/retail/download.py          # Downloads all files
 
Output:
    data/raw/instacart/orders.csv
    data/raw/instacart/order_products__prior.csv
    data/raw/instacart/order_products__train.csv
    data/raw/instacart/products.csv
    data/raw/instacart/aisles.csv
    data/raw/instacart/departments.csv
"""

import shutil
from tqdm import tqdm
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_URL = "yasserh/instacart-online-grocery-basket-analysis-dataset"
OUTPUT_DIR  = Path("data/raw/retail")

KEEP_FILES = {
    "order_products__prior.csv",
    "products.csv",
    "aisles.csv",
    "departments.csv",
}

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def authenticate() -> KaggleApi:
    api = KaggleApi()
    api.authenticate()
    return api

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Authenticating with Kaggle...\n")
    try:
        api = authenticate()
    except Exception as e:
        print(f"ERROR: Kaggle authentication failed: {e}")
        print("Ensure kaggle.json is placed in ~/.kaggle/kaggle.json")
        exit(1)


    try:
        # Downloads and unzips into OUTPUT_DIR
        api.dataset_download_files(
            BASE_URL,
            path    = OUTPUT_DIR,
            unzip   = True,
            quiet = False
        )

        #Before removing unneeded files
        total_size_mb = sum(f.stat().st_size for f in OUTPUT_DIR.iterdir() if f.is_file()) / (1024 * 1024)
        print(f"Dataset download completed with files totalling size: ({total_size_mb:.1f} MB)")
        print(f"The dataset is stored at {OUTPUT_DIR}")

        for f in OUTPUT_DIR.iterdir():
            if f.is_file() and f.name not in KEEP_FILES:
                f.unlink()
                print(f"  Removed: {f.name}")

        #After removing unneeded files
        total_size_mb = sum(f.stat().st_size for f in OUTPUT_DIR.iterdir() if f.is_file()) / (1024 * 1024)
        print(f"New dataset file is: ({total_size_mb:.1f} MB)")

    except Exception as e:
        print(f"An error occurred while downloading dataset from {BASE_URL}: {e}\n")
        print(f"Clearing the output directoy {OUTPUT_DIR} to remove any partial files...")

        shutil.rmtree(OUTPUT_DIR)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    main()