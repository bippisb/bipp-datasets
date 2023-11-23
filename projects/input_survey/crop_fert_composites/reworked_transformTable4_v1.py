# %%
import pandas as pd
from pathlib import Path
import re
import io

# %%
PROJECT_DIR = Path(__file__).parent.parent
DATA_DIR = PROJECT_DIR / "interim" / "2016"
DESTINATION_DIR = PROJECT_DIR / "reworked_transformData"

# %%
csv_files = list(DATA_DIR.rglob("**/TABLE4-*/*.csv"))

# %%

size_mapping = {
    "MARGINAL (BELOW 1.0)": ("MARGINAL", "BELOW 1.0", "NaN", "NaN", "NaN"),
    "SMALL (1.0 - 1.99)": ("SMALL", "1.0 - 1.99", "NaN", "NaN", "NaN"),
    "SEMI-MEDIUM (2.0 - 3.99)": ("SEMI-MEDIUM", "2.0 - 3.99", "NaN", "NaN", "NaN"),
    "MEDIUM (4.0 - 9.99)": ("MEDIUM", "4.0 - 9.99", "NaN", "NaN", "NaN"),
    "LARGE (10 AND ABOVE)": ("LARGE", "10 AND ABOVE", "NaN", "NaN", "NaN"),
    "ALL GROUPS": ("ALL", "ALL", "NaN", "NaN", "NaN"),
}


def main():
    for i in range(len(csv_files)):
        data = pd.read_csv(csv_files[i])
        
        farmSizeCategory_list = []
        farmSizeClass_list = []
        waterSource_list = []

        sizeGroup_col = data["SIZE GROUP(HA)"]
        
        sizeStage = "None" # variable to keep track of the farm size category
        sizeArea = "0" # variable to keep track of the farm size range

        for element in sizeGroup_col:
            
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
                sizeStage, sizeArea, farmSizeCat, farmSizeClass, waterSource = size_mapping.get(element, (sizeStage, sizeArea, "NaN", "NaN", "NaN"))
                farmSizeCategory_list.append(farmSizeCat)
                farmSizeClass_list.append(farmSizeClass)
                waterSource_list.append(waterSource)
            
        
        data["Farm_size_category"] = farmSizeCategory_list
        data["Farm_size_class"] = farmSizeClass_list
        data["Water_source"] = waterSource_list

        for j in range(0,len(data)):
            if j%4 == 0: 
                data.drop(index=[j], inplace=True)
        
        dest = DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR).parent
        dest.mkdir(parents=True, exist_ok=True)
        data.to_csv(DESTINATION_DIR / "2016" / csv_files[i].relative_to(DATA_DIR), index=False)


if __name__ == "__main__":
    main()

