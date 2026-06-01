#!/usr/bin/env python3
"""
Download raw CSV files from Google Drive into raw_data/ folder.
Requires gdown: pip install gdown

Usage:
    python3 download_raw_data.py

Instructions to get shareable links:
1. Go to Google Drive folder: https://drive.google.com/drive/u/0/folders/1oPBELdKx2gcZ6Uamur45729EXIt7j716
2. For each file, right-click > Share > Copy link
3. Replace the FILE_URLS below with the actual shareable links
"""

import os
import sys
from pathlib import Path

try:
    import gdown
except ImportError:
    print("gdown not installed. Installing now...")
    os.system("pip install gdown")
    import gdown

RAW_DATA_DIR = Path("raw_data")
RAW_DATA_DIR.mkdir(exist_ok=True)

# Replace these with actual Google Drive shareable links
# Get links by: right-click file in Drive > Share > Copy link
FILE_URLS = {
    "stores.csv": None,  # Already present
    "customers.csv": "https://drive.google.com/uc?id=1PfT5Pnq23cW0Tu7n_gWDGuE-SrNzZJMT",
    "orders.csv": "https://drive.google.com/uc?id=1Yn555ETEzYcIYQHp4BrAR9Fheb34jtiP",
    "order_items.csv": "https://drive.google.com/uc?id=1DofYhncoMhz-A0d9fH9nu1EjUOfr_J28",
    "products.csv": "https://drive.google.com/uc?id=1Slm3xeixVInwsxSDQqaB0TGjTDmqtNFr",
    "payments.csv": "https://drive.google.com/uc?id=1HF_rtrNqa_7am2edmJY62fYhTRYDURHp",
    "clickstream.csv": "https://drive.google.com/uc?id=1W3GTtDcXsPcRhN2AFherwHnJ5Na6CJzf",
    "inventory.csv": "https://drive.google.com/uc?id=1_ksrmr3hg-f5oRGHSn06zK06CxFnwyij",
}

def download_file(filename, url):
    """Download a file from Google Drive."""
    if not url:
        print(f"⏭️  Skipping {filename} (no URL provided)")
        return False
    
    output_path = RAW_DATA_DIR / filename
    
    if output_path.exists():
        print(f"✅ {filename} already exists, skipping")
        return True
    
    print(f"⬇️  Downloading {filename}...")
    try:
        gdown.download(url, str(output_path), quiet=False)
        print(f"✅ Downloaded {filename}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")
        return False

def main():
    print("=" * 60)
    print("Google Drive Raw Data Downloader")
    print("=" * 60)
    
    # Check if any URLs are configured
    configured_urls = [v for v in FILE_URLS.values() if v is not None]
    if not configured_urls:
        print("\n⚠️  No download URLs configured!")
        print("\nTo configure, edit this script and add Google Drive shareable links.")
        print("\nExample:")
        print('  FILE_URLS = {')
        print('      "customers.csv": "https://drive.google.com/uc?id=FILE_ID",')
        print('      ...')
        print("  }")
        print("\nTo get shareable links:")
        print("1. Open: https://drive.google.com/drive/u/0/folders/1oPBELdKx2gcZ6Uamur45729EXIt7j716")
        print("2. Right-click each file > Share > Copy link")
        print("3. Paste into FILE_URLS dict above")
        sys.exit(1)
    
    print(f"\nTarget directory: {RAW_DATA_DIR.resolve()}\n")
    
    downloaded = 0
    failed = 0
    skipped = 0
    
    for filename, url in FILE_URLS.items():
        if download_file(filename, url):
            if url:  # Only count as downloaded if URL was present
                downloaded += 1
            else:
                skipped += 1
        else:
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Summary: {downloaded} downloaded, {skipped} skipped, {failed} failed")
    print("=" * 60)
    
    # List all files in raw_data
    files = list(RAW_DATA_DIR.glob("*.csv"))
    print(f"\nFiles in {RAW_DATA_DIR}/:")
    for f in sorted(files):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  ✓ {f.name} ({size_mb:.2f} MB)")

if __name__ == "__main__":
    main()
