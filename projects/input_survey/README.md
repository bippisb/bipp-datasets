# Data Consolidation Project

This project includes scripts for consolidating agricultural survey data from the [inputsurvey.dacnet.nic.in](https://inputsurvey.dacnet.nic.in/) website. The data consolidation is based on three codebooks, each specifying the required data points from different tables. The project follows a process of cleaning, transforming, and consolidating the data according to the defined codebooks.

## Project Structure

- `consolidate_data.py`: Script to consolidate data based on the specified codebooks.
- `data_extraction_script.py`: Script to automate data extraction from the website.
- `merge_data.py`: Script to merge multiple JSON files into a single file.
- `README.md`: Project documentation (you are here).

## Codebooks

1. **Crop and Fertilizer Codebook:**
   - Codebook Name: Crop_fert
   - Tables Needed: Table4A

2. **Crop and Fertilizer Composites Codebook:**
   - Codebook Name: Crop_fert_Composites
   - Tables Needed: Table4, Table5E, 5F, 5G, 5H, 5I, 5K, 5J, 5L, 5LA

3. **Non-Crop Codebook:**
   - Codebook Name: NonCrop
   - Tables Needed: Table2A, Table2B, Table8, Table9A

## Requirements

- Python 3.x
- Additional Python packages listed in `requirements.txt`

## Usage

1. **Consolidate Data (`consolidate_data.py`):**
   - Consolidates data based on the specified codebooks.
   - Output: JSON files corresponding to each codebook.

2. **Data Extraction (`data_extraction_script.py`):**
   - Automates the process of extracting data from the website.
   - Requires the use of Selenium and a WebDriver (Microsoft Edge in this case).
   - Downloads data to the `data/raw/downloads` directory.

3. **Merge Data (`merge_data.py`):**
   - Merges multiple JSON files into a single file.

## How to Run

## Notes

- Make sure to have the appropriate WebDriver for Selenium set up. (In this case, Microsoft Edge is used.)
- Ensure that the `data/raw` and `data/raw/downloads` directories exist before running scripts.

Feel free to contribute, report issues, or suggest improvements!