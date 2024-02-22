# %%
# Import necessary libraries
import pandas as pd
from pathlib import Path
import re
import io

# %%
# Define project directories
PROJECT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_DIR /"data"/"interim" / "2016"
DESTINATION_DIR = PROJECT_DIR /"data"/"processed"

# %%
# Create a list of CSV files from different TABLE5 series tables
csv_filesE = list(DATA_DIR.rglob("**/TABLE5E*.csv"))
csv_filesF = list(DATA_DIR.rglob("**/TABLE5F*.csv"))
csv_filesG = list(DATA_DIR.rglob("**/TABLE5G*.csv"))
csv_filesH = list(DATA_DIR.rglob("**/TABLE5H*.csv"))
csv_filesI = list(DATA_DIR.rglob("**/TABLE5I*.csv"))
csv_filesJ = list(DATA_DIR.rglob("**/TABLE5J*.csv"))
csv_filesK = list(DATA_DIR.rglob("**/TABLE5K*.csv"))
csv_filesL = list(DATA_DIR.rglob("**/TABLE5L*.csv"))
csv_filesLA = list(DATA_DIR.rglob("**/TABLE5LA*.csv"))

csv_files = csv_filesE + csv_filesF + csv_filesG + csv_filesH + csv_filesI + csv_filesJ + csv_filesK + csv_filesL + csv_filesLA

# %%

# Define a mapping for transforming farm size categories
size_mapping = {
    "MARGINAL (BELOW 1.0)": ("MARGINAL", "BELOW 1.0", "NaN", "NaN", "NaN"),
    "SMALL (1.0 - 1.99)": ("SMALL", "1.0 - 1.99", "NaN", "NaN", "NaN"),
    "SEMI-MEDIUM (2.0 - 3.99)": ("SEMI-MEDIUM", "2.0 - 3.99", "NaN", "NaN", "NaN"),
    "MEDIUM (4.0 - 9.99)": ("MEDIUM", "4.0 - 9.99", "NaN", "NaN", "NaN"),
    "LARGE (10 AND ABOVE)": ("LARGE", "10 AND ABOVE", "NaN", "NaN", "NaN"),
    "ALL GROUPS": ("ALL", "ALL", "NaN", "NaN", "NaN"),
}

# %%
def main():
    for i in range(len(csv_files)):
        # Read the CSV file into a DataFrame
        data = pd.read_csv(csv_files[i])
        
        # Lists to store the transformed values
        farmSizeCategory_list = []
        farmSizeClass_list = []
        waterSource_list = []

        # Extract the "SIZE GROUP(HA)" column
        sizeGroup_col = data["SIZE GROUP(HA)"]
        
        # Variables to keep track of farm size category and range
        sizeStage = "None"
        sizeArea = "0"

        # Loop through each element in "SIZE GROUP(HA)"
        for element in sizeGroup_col:
            
            # Handle cases where "I", "UI", or "T" is encountered
            if element == "I":
                waterSource_list.append("Irrigated")
                farmSizeCategory_list.append(sizeStage)
                farmSizeClass_list.append(sizeArea)
            elif element == "UI":
                waterSource_list.append("UnIrrigated")
                farmSizeCategory_list.append(sizeStage)
                farmSizeClass_list.append(sizeArea)
            elif element == "T":
                waterSource_list.append("Total")
                farmSizeCategory_list.append(sizeStage)
                farmSizeClass_list.append(sizeArea)
            else:
                # Use the mapping to get the transformed values
                sizeStage, sizeArea, farmSizeCat, farmSizeClass, waterSource = size_mapping.get(element, (sizeStage, sizeArea, "NaN", "NaN", "NaN"))
                farmSizeCategory_list.append(farmSizeCat)
                farmSizeClass_list.append(farmSizeClass)
                waterSource_list.append(waterSource)
            
        # Add the transformed columns to the DataFrame
        data["Farm_size_category"] = farmSizeCategory_list
        data["Farm_size_class"] = farmSizeClass_list
        data["Water_source"] = waterSource_list

        # Drop rows with indices divisible by 4 (1 out of 4 rows)
        for j in range(0, len(data)):
            if j % 4 == 0:
                data.drop(index=[j], inplace=True)
        
        # Define the destination directory, create directories if not exist, and write to CSV
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        
        # Pick only the last 18 rows (which have the total values of each column)
        finalData = data.iloc[-18:]
        finalData.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)

# %%
if __name__ == "__main__":
    main()
