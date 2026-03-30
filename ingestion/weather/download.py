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
    python ingestion/weather/download.py --year 2022                        # Downloads specific year
    python ingestion/weather/download.py --year-start 2020 --year-end 2023  # Downloads a range

Output:
    data/raw/weather/YYYY.tar.gz
"""

import argparse
import requests
from tqdm import tqdm
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_URL   = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
OUTPUT_DIR = Path("data/raw/weather")
CHUNK_SIZE = 8192  # bytes — streams download so large files don't fill RAM

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def build_download_url(year: int) -> str:
    return f"{BASE_URL}/{year}.tar.gz"

def build_destination_path(year: int) -> Path:
    return OUTPUT_DIR / f"{year}.tar.gz"

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download NOAA GSOD weather archive files")
    parser.add_argument("--year",       type=int, default=2023, help="Single year to download (default: 2023)")
    parser.add_argument("--year-start", type=int, default=None, help="First year in range")
    parser.add_argument("--year-end",   type=int, default=None, help="Last year in range")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build the year range
    if args.year_start and args.year_end:
        if args.year_start > args.year_end:
            print(f"ERROR: Year start ({args.year_start}) is greater than year end ({args.year_end})")
            exit(1)
        years = range(args.year_start, args.year_end + 1)
    else:
        #Default to just the given year if no range given
        years = [args.year]

    total_file_size = 0

    for year in years:
        url = build_download_url(year)
        destination = build_destination_path(year)

        print(f"Beginning download of NOAA GSOD dataset for {year} ...")

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))

            with open(destination, "wb") as file:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=destination.name) as bar:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))

            file_size_mb = destination.stat().st_size / (1024 * 1024)
            total_file_size += file_size_mb
            print(f"Download completed for file {destination}: ({file_size_mb:.1f} MB)\n")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while downloading NOAA data for {year}: {e}\n")

            if destination.exists():
                destination.unlink()

    print(f"Download of files completed. Total size of all files downloaded: ({total_file_size:.1f} MB)\n")


if __name__ == "__main__":
    main()