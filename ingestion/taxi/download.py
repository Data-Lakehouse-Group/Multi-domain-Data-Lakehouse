"""
NYC Taxi TLC Data Downloader
============================
Downloads raw Parquet files from the TLC website and saves them
to a local staging directory before Bronze ingestion.
 
Usage: (add --debug at the end of each for local testing)
    python ingestion/taxi/download.py                    # Downloads 2023 full year by default
    python ingestion/taxi/download.py --year 2022        # Downloads specific year
    python ingestion/taxi/download.py --year 2023 --month-start 1 --month-end 1   # Downloads January only
    python ingestion/taxi/download.py --year 2023 --month-start 1 --month-end 5   # Downloads January to May
 
Output:
    data/raw/taxi/yellow_tripdata_YYYY-MM.parquet
"""

import calendar
import argparse
import requests
from tqdm import tqdm
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
 
BASE_URL    = "https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_DIR_DEBUG    = Path("data/raw/taxi")
OUTPUT_DIR_PROD     = Path("/opt/airflow/data/raw/taxi")
CHUNK_SIZE  = 8192  # bytes — streams download so large files don't fill RAM

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def build_download_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"

def build_destination_path(year: int, month: int, is_debug: bool) -> Path:
    if is_debug:
        return OUTPUT_DIR_DEBUG / f"yellow_tripdata_{year}-{month:02d}.parquet"
    else:
        return OUTPUT_DIR_PROD / f"yellow_tripdata_{year}-{month:02d}.parquet"

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    #Sets up the parser for the CLI call of this file
    parser = argparse.ArgumentParser(description="Download NYC Taxi TLC Parquet files")
    parser.add_argument("--year",  type=int, default=2023, help="Year to download (default: 2023)")
    parser.add_argument("--month-start", type=int, default=None, help="First month in range. Omit for full year")
    parser.add_argument("--month-end", type=int, default=None, help="Last month in range. Omit for full year")
    parser.add_argument("--debug" ,action="store_true", help="Set to true if running locally")

    args = parser.parse_args()

    #Makes the directory for the output files if it doesnt exist
    if args.debug is True:
        OUTPUT_DIR_DEBUG.mkdir(parents=True, exist_ok=True)
    else:
        OUTPUT_DIR_PROD.mkdir(parents=True, exist_ok=True)

    #Make the month range from arguments
    if args.month_start is not None and args.month_end is not None:
        if(args.month_start > args.month_end):
            print(f"ERROR: Month start range ({args.month_start}) is greater than month end range({args.month_end})")
            exit(1)

        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)
        
    total_file_size = 0

    #Loop through each month in the range given
    for month in months:
        year = args.year
        month_name = calendar.month_name[month]

        url = build_download_url(year, month)
        destination = build_destination_path(year, month, args.debug)

        print(f"Beginning download of taxi dataset for {month_name} , {year} ")

        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status() # Check if request was successful

            #Open the file to download the data to
            total_size = int(response.headers.get('content-length', 0))

            with open(destination, 'wb') as file:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=destination.name) as bar:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))

            file_size_mb = destination.stat().st_size / (1024 * 1024)
            total_file_size += file_size_mb
            print(f"Download completed for file {destination}: ({file_size_mb:.1f} MB)' \n")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while downloading taxi dataset for {month_name} , {year}: {e} \n")
            
            if destination.exists():
                destination.unlink()

    print(f"Download of files completed, total size of all files downloaed was: ({total_file_size:.1f} MB)' \n")


if __name__ == "__main__":
    main()