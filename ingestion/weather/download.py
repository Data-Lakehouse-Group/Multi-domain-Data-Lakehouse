"""
NOAA Weather Data Downloader
============================
Downloads Global Surface Summary of Day (GSOD) tar.gz archives from NOAA's
HTTPS data portal and saves them to a local staging directory before Bronze
ingestion.

Data source: https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/
Each archive contains one CSV per weather station for the given year.

Usage:
    python ingestion/weather/download.py                                    # Downloads 2023 by default
    python ingestion/weather/download.py --year-start 2020 --year-end 2023  # Downloads a range

Output:
    data/raw/weather/YYYY.tar.gz
"""
import os
import argparse
import requests
from tqdm import tqdm


import pyarrow.fs as pafs

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_URL   = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
DESTINATION_URI = "raw/weather"
CHUNK_SIZE = 8192  # bytes — streams download so large files don't fill RAM

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

def build_download_url(year: int) -> str:
    return f"{BASE_URL}/{year}.tar.gz"

def get_s3_filesystem() -> pafs.S3FileSystem:
    endpoint = STORAGE_OPTIONS["endpoint_url"].replace("http://", "").replace("https://", "")
    scheme   = "https" if STORAGE_OPTIONS.get("aws_use_ssl", "false") == "true" else "http"

    return pafs.S3FileSystem(
        endpoint_override = endpoint,
        access_key        = STORAGE_OPTIONS["aws_access_key_id"],
        secret_key        = STORAGE_OPTIONS["aws_secret_access_key"],
        scheme            = scheme,
    )

def build_destination_path(year: int) -> str:
    return f"{DESTINATION_URI}/{year}.tar.gz"

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download NOAA GSOD weather archive files")
    parser.add_argument("--year-start", type=int, default=2023, help="First year in range")
    parser.add_argument("--year-end",   type=int, default=2023, help="Last year in range")
    args = parser.parse_args()


    # Build the year range
    if args.year_start > args.year_end:
            print(f"ERROR: Year start ({args.year_start}) is greater than year end ({args.year_end})")
            exit(1)
    years = range(args.year_start, args.year_end + 1)

    total_file_size = 0

    fs = get_s3_filesystem() #Gets the MinIO (S3) file configs

    for year in years:
        url = build_download_url(year)
        destination = build_destination_path(year)

        print(f"Beginning download of NOAA GSOD dataset for {year} ...")

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))

            with fs.open_output_stream(destination) as f:
                with tqdm(
                    total      = total_size,
                    unit       = "B",
                    unit_scale = True,
                    desc       = f"{year}.tar.gz"
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
            print(f"[OK] Download completed for file {destination}: ({file_size_mb:.1f} MB)\n")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] An error occurred while downloading NOAA data for {year}: {e}\n")

    print(f"[OK] Download of files completed. Total size of all files downloaded: ({total_file_size:.1f} MB)\n")

if __name__ == "__main__":
    main()