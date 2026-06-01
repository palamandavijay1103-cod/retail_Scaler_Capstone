# Google Drive Raw Data Download Setup

## Quick Start

1. **Install gdown** (handles Google Drive downloads):
   ```bash
   pip install gdown
   ```

2. **Get shareable links for each file**:
   - Open: https://drive.google.com/drive/u/0/folders/1oPBELdKx2gcZ6Uamur45729EXIt7j716
   - For each CSV file, right-click > Share > Copy link
   - Each link will look like: `https://drive.google.com/file/d/FILE_ID/view?usp=sharing`

3. **Configure the download script**:
   - Open `download_raw_data.py`
   - Find the `FILE_URLS` dictionary
   - For each file, replace the URL:
   ```python
   FILE_URLS = {
       "stores.csv": None,  # Already present, skip
       "customers.csv": "https://drive.google.com/uc?id=YOUR_FILE_ID",
       "orders.csv": "https://drive.google.com/uc?id=YOUR_FILE_ID",
       "order_items.csv": "https://drive.google.com/uc?id=YOUR_FILE_ID",
       "products.csv": "https://drive.google.com/uc?id=YOUR_FILE_ID",
       "payments.csv": "https://drive.google.com/uc?id=YOUR_FILE_ID",
       "clickstream.csv": "https://drive.google.com/uc?id=YOUR_FILE_ID",
       "wallet.csv": "https://drive.google.com/uc?id=YOUR_FILE_ID",
   }
   ```

4. **Run the download script**:
   ```bash
   python3 download_raw_data.py
   ```

## How to Extract FILE_ID from Google Drive Link

If you have a shareable link like:
```
https://drive.google.com/file/d/1aBcDeFgHiJkLmNoPqRsTuVwXyZ/view?usp=sharing
```

The `FILE_ID` is the long string between `/d/` and `/view`:
```
1aBcDeFgHiJkLmNoPqRsTuVwXyZ
```

Then format it for gdown:
```
https://drive.google.com/uc?id=1aBcDeFgHiJkLmNoPqRsTuVwXyZ
```

## Troubleshooting

- **Permission denied**: Ensure the Google Drive folder is shared with you
- **File not found**: Double-check the FILE_ID is correct
- **Timeout**: The file may be large; gdown retries automatically
- **Already have files**: Script skips files that already exist in `raw_data/`

## Expected Files (8 total)

1. ✅ stores.csv (already present)
2. customers.csv
3. orders.csv
4. order_items.csv
5. products.csv
6. payments.csv
7. clickstream.csv
8. wallet.csv
