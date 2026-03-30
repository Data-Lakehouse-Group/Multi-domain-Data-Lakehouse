"""
GitHub Archive Downloader
==========================
Downloads hourly JSON.GZ event dumps from GH Archive (https://www.gharchive.org/)
and saves them to a local staging directory before Bronze ingestion.

Each file covers one hour of public GitHub events (PushEvent, PullRequestEvent,
IssuesEvent, etc.) and is already gzip-compressed — no extra compression step needed.

WARNING: These datasets are large and reccomended to only use hour ranges for one day

Usage:
    python ingestion/github_archive/download.py                                                               # Default Downloads all of 2023-02-01-1, Hour 1 of 2nd Feb 2023
    python ingestion/github_archive/download.py --date-hour 2024-01-01-2                                      # Downloads all of 2023-02-01-2, Hour 1 of 2nd Feb 2023
    python ingestion/github_archive/download.py --date-hour-start 2024-01-01-1 --date-hour-end 2024-01-01-2   # Hour range for same day
Output:
    data/raw/github/YYYY-MM-DD-H.json.gz   (one file per hour)
"""

import argparse
from tqdm import tqdm
import requests
from pathlib import Path
from datetime import datetime, date

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_URL   = "https://data.gharchive.org"
OUTPUT_DIR = Path("data/raw/github_archive")
CHUNK_SIZE = 8192   # bytes — streams download so large files don't fill RAM

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def build_download_url(target_date: date, hour: int) -> str:
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")   
    day = target_date.strftime("%d")
    return f"{BASE_URL}/{str(year)}-{str(month)}-{str(day)}-{str(hour)}.json.gz"

def build_destination_path(target_date: date, hour: int) -> Path:
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")   
    day = target_date.strftime("%d")
    return OUTPUT_DIR / f"{str(year)}-{str(month)}-{str(day)}-{str(hour)}.json.gz"

def parse_date(date_str: str) -> datetime:
    dt = datetime.strptime(date_str, "%Y-%m-%d-%H")
    return dt

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download GH Archive hourly JSON.GZ files")
    parser.add_argument("--date-hour",       type=str, default='2023-02-01-1', help="Single hour for day  (YYYY-MM-DD-HR)")
    parser.add_argument("--date-hour-start", type=str, default=None, help="Start of date hour range (YYYY-MM-DD-HR)")
    parser.add_argument("--date-hour-end",   type=str, default=None, help="End of date hour range   (YYYY-MM-DD-HR)")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build the date hour range -------------------------------------------------------
    if args.date_hour_start and args.date_hour_end:
        date_hour_start = parse_date(args.date_hour_start)
        date_hour_end = parse_date(args.date_hour_end)
        
        if date_hour_start.date() > date_hour_end.date():
            print(f"ERROR: Start date is not the same ({date_hour_start.date()}) as end date  ({date_hour_end.date()})")
            exit(1)
        elif date_hour_start > date_hour_end:
            print(f"ERROR: date-hour-start ({args.date_hour_start}) is after date-hour-end ({args.date_hour_end})")
            exit(1)
 
        date = date_hour_start.date()
        start_hour = date_hour_start.strftime("%H")
        end_hour = date_hour_end.strftime("%H")
        hours = range(int(start_hour), int(end_hour)+1)
    else:
        date_hour = parse_date(args.date_hour)
        date = date_hour.date()
        hour = date_hour.strftime("%H")
        hours = range(int(hour), int(hour)+1)

    total_file_size = 0
    total_files     = len(hours)

    print(f"Preparing to download {total_files} hourly file.\n")

    for hour in hours:
        url         = build_download_url(date, hour)
        destination = build_destination_path(date, hour)

        print(f"  Downloading {date} hour={hour:02d} ...", end=" ", flush=True)
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
            print(f"Download completed for file {destination}: ({file_size_mb:.1f} MB)' \n")
        except requests.exceptions.RequestException as e:
            print(f"ERROR — {e}")

            if destination.exists():
                destination.unlink()


    print(f"Download of files completed. Total size of all files downloaded: ({total_file_size:.1f} MB)\n")


if __name__ == "__main__":
    main()