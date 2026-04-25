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
import os
import calendar
import argparse
import requests
from tqdm import tqdm
import pyarrow.fs as pafs

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
 
BASE_URL    = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DESTINATION_URI = "raw/taxi"
CHUNK_SIZE  = 8192  # bytes — streams download so large files don't fill RAM

# MinIO connection
STORAGE_OPTIONS = {
    "endpoint_url"              : os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "aws_access_key_id"         : os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "aws_secret_access_key"     : os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "allow_http"                : "true",
    "aws_region"                : "us-east-1",
    "aws_s3_allow_unsafe_rename": "true",
}

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def build_download_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"

def get_s3_filesystem() -> pafs.S3FileSystem:
    endpoint = STORAGE_OPTIONS["endpoint_url"].replace("http://", "").replace("https://", "")
    scheme   = "https" if STORAGE_OPTIONS.get("aws_use_ssl", "false") == "true" else "http"

    return pafs.S3FileSystem(
        endpoint_override = endpoint,
        access_key        = STORAGE_OPTIONS["aws_access_key_id"],
        secret_key        = STORAGE_OPTIONS["aws_secret_access_key"],
        scheme            = scheme,
    )

def build_destination_path(year: int, month: int) -> str:
    return f"{DESTINATION_URI}/{year}-{month:02d}.parquet"

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

    #Make the month range from arguments
    if args.month_start is not None and args.month_end is not None:
        if(args.month_start > args.month_end):
            print(f"ERROR: Month start range ({args.month_start}) is greater than month end range({args.month_end})")
            exit(1)

        months = range(args.month_start, args.month_end + 1)
    else:
        months = range(1, 13)
        
    total_file_size = 0

    fs = get_s3_filesystem() #Gets the MinIO (S3) file configs

    #Loop through each month in the range given
    for month in months:
        year = args.year
        month_name = calendar.month_name[month]

        url = build_download_url(year, month)
        destination = build_destination_path(year, month)

        print(f"Beginning download of taxi dataset for {month_name} , {year} ")

        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status() # Check if request was successful

            #Open the file to download the data to
            total_size = int(response.headers.get('content-length', 0))

            with fs.open_output_stream(destination) as f:
                with tqdm(
                    total      = total_size,
                    unit       = "B",
                    unit_scale = True,
                    desc       = f"yellow_tripdata_{year}-{month:02d}.parquet"
                ) as bar:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))

            #Gets the size of the file
            try:
                info = fs.get_file_info(destination)
                file_size_mb = info.size / (1024 * 1024)
            except Exception:
                file_size_mb = 0.0

            total_file_size += file_size_mb
            print(f"[OK] Download completed for file {destination}: ({file_size_mb:.1f} MB)' \n")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] An error occurred while downloading taxi dataset for {month_name} , {year}: {e} \n")
            

    print(f"[OK] Download of files completed, total size of all files downloaed was: ({total_file_size:.1f} MB)' \n")


if __name__ == "__main__":
    main()