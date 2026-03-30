"""
NYC Taxi TLC Data Downloader
============================
Downloads raw Parquet files from the TLC website and saves them
to a local staging directory before Bronze ingestion.
 
Usage:
    python taxi_data_download.py                    # Downloads 2023 full year by default
    python taxi_data_download.py --year 2022        # Downloads specific year
    python taxi_data_download.py --year 2023 --month-start 1 --month-end 1   # Downloads January only
    python taxi_data_download.py --year 2023 --month-start 1 --month-end 5   # Downloads January to May
 
Output:
    data/raw/taxi/yellow_tripdata_YYYY-MM.parquet
"""

import calendar
import argparse
import requests
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
 
BASE_URL    = "https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_DIR  = Path("data/raw/taxi")
CHUNK_SIZE  = 8192  # bytes — streams download so large files don't fill RAM

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def build_download_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"

def build_destination_path(year: int, month: int) -> Path:
    return OUTPUT_DIR / f"yellow_tripdata_{year}-{month:02d}.parquet"

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    #Sets up the parser for the CLI call of this file
    parser = argparse.ArgumentParser(description="Download NYC Taxi TLC Parquet files")
    parser.add_argument("--year",  type=int, default=2023, help="Year to download (default: 2023)")
    parser.add_argument("--month-start", type=int, default=None, help="First month in range. Omit for full year")
    parser.add_argument("--month-end", type=int, default=None, help="Last month in range. Omit for full year")


    args = parser.parse_args()

    #Makes the directory for the output files if it doesnt exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    #Make the month range from arguments
    if args.month_start and args.month_end:
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
        destination = build_destination_path(year, month)

        print(f"Beginning download of taxi dataset for {month_name} , {year} ")

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status() # Check if request was successful

            #Open the file to download the data to
            with open(destination, 'wb') as file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        file.write(chunk)

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