"""NYC Taxi TLC Data Downloader – full 2024"""
import calendar
import argparse
import requests
from tqdm import tqdm
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_DIR = Path("data/raw/taxi")
CHUNK_SIZE = 8192

def build_download_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"

def build_destination_path(year: int, month: int) -> Path:
    return OUTPUT_DIR / f"yellow_tripdata_{year}-{month:02d}.parquet"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2024, help="Year to download (default: 2024)")
    parser.add_argument("--month-start", type=int, default=1)
    parser.add_argument("--month-end", type=int, default=12)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_size = 0

    for month in range(args.month_start, args.month_end + 1):
        url = build_download_url(args.year, month)
        dest = build_destination_path(args.year, month)
        month_name = calendar.month_name[month]

        print(f"Downloading {month_name} {args.year} ...")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            total = int(response.headers.get('content-length', 0))
            with open(dest, 'wb') as f:
                with tqdm(total=total, unit='B', unit_scale=True, desc=dest.name) as bar:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
            size_mb = dest.stat().st_size / (1024 * 1024)
            total_size += size_mb
            print(f"✓ {dest.name} ({size_mb:.1f} MB)")
        except Exception as e:
            print(f"Error: {e}")
            if dest.exists():
                dest.unlink()

    print(f"Total downloaded: {total_size:.1f} MB")

if __name__ == "__main__":
    main()