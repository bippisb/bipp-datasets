# %%
import os
from datetime import datetime

import openpyxl as xl
import pandas as pd

# %%
# Folder paths
dir_path = os.getcwd()
data_path = os.path.join(dir_path, "data")
raw_path = os.path.join(data_path, "raw")
interim_path = os.path.join(data_path, "interim")
processed_path = os.path.join(data_path, "processed")
# %%
# get list of files
format = "old format"
folder_path = os.path.join(raw_path, format)
interim_folder = os.path.join(interim_path, format)
years = os.listdir(folder_path)
# %%
for year in years[-4:]:
    year_path = os.path.join(folder_path, year)
    file_list = [
        file
        for dirpath, dirnames, files in os.walk(year_path)
        for file in files
        if file.endswith(".xlsx")
    ]

    # iterate through each file
    for file in file_list:
        file_path = os.path.join(year_path, file)
        print("\033[1;32mProcessing file: \033[0m", file)
        excel_file = pd.read_excel(file_path, sheet_name=None)
        wb = xl.load_workbook(file_path)
        sheet_names = wb.sheetnames

        ic_sheet_name = next(
            (name for name in sheet_names if "indian" in name.lower()), None
        )
        if ic_sheet_name:
            ic = excel_file[ic_sheet_name]
            if "cin" in [str(value).strip().lower() for value in ic.columns]:
                ic.columns = ic.columns.str.lower().str.strip()
                ic_sel = ic.loc["cin":, :]
            else:
                in_index = None
                for index, row in ic.iterrows():
                    if "cin" in [
                        str(value).strip().lower() for value in row.values
                    ]:
                        in_index = index
                        break
                if in_index is not None:
                    ic.columns = (
                        ic.iloc[in_index].astype(str).str.lower().values
                    )
                    ic_sel = ic.iloc[in_index + 1 :]
                    ic_sel.columns = ic_sel.columns.str.lower().str.strip()
                    ic_sel = ic_sel.loc["cin":, :]
            for col in ic_sel.columns:
                if "month" in col:
                    if isinstance(ic_sel[col].iloc[0], datetime):
                        print(ic_sel[col].iloc[0])
                        month = ic_sel[col].iloc[0].month
                    else:
                        month = ic_sel[col].iloc[0]
                        print(month)
                    date_string = f"{month}_{year}"
                    print(f"\033[1;31m Date : {date_string}\033[0m")
            ic_sel.reset_index(drop=True, inplace=True)

            print(f"Sheet:{ic_sheet_name}")
            print(f"Rows:{ic.shape[0]},    Columns:{ic.shape[1]}")
        else:
            print("\033[1;31m No Indian Company sheet\033[0m")

        llp_sheet_name = next(
            (name for name in sheet_names if "llp" in name.lower()), None
        )
        if llp_sheet_name:
            llp = excel_file[llp_sheet_name]
            if "llpin" in [
                str(value).strip().lower() for value in llp.columns
            ]:
                llp.columns = llp.columns.str.lower().str.strip()
                llp = llp.loc["llpin":, :]
            else:
                in_index = None
                for index, row in llp.iterrows():
                    if "llpin" in [
                        str(value).strip().lower() for value in row.values
                    ]:
                        in_index = index
                        break
                if in_index is not None:
                    llp.columns = (
                        llp.iloc[in_index].astype(str).str.lower().values
                    )
                    llp = llp.iloc[in_index + 1 :]
                    llp.columns = llp.columns.str.lower().str.strip()
                    llp = llp.loc["llpin":, :]
            llp.reset_index(drop=True, inplace=True)

            print(f"Sheet:{llp_sheet_name}")
            print(f"Rows:{llp.shape[0]},    Columns:{llp.shape[1]}")
        else:
            print("\033[1;31m No LLP sheet\033[0m")

        foregin_sheet_name = next(
            (name for name in sheet_names if "foreign" in name.lower()), None
        )
        col_name = ["fcin", "cin", "fcrn"]
        if foregin_sheet_name:
            foreign = excel_file[foregin_sheet_name]
            in_index = None
            for f in col_name:
                for index, row in foreign.iterrows():
                    if f in [
                        str(value).strip().lower() for value in row.values
                    ]:
                        in_index = index

            if in_index is not None:
                foreign.columns = (
                    foreign.iloc[in_index]
                    .astype(str)
                    .str.lower()
                    .str.strip()
                    .values
                )
                for f in col_name:
                    if f in foreign.columns:
                        foreign = foreign.loc[f:, :]
                        foreign = foreign.iloc[in_index + 1 :]
                        break
            foreign.reset_index(drop=True, inplace=True)

            print(f"Sheet:{foregin_sheet_name}")
            print(f"Rows:{foreign.shape[0]},    Columns:{foreign.shape[1]}")
        else:
            print("\033[1;31m No Forign Company sheet\033[0m")

        pr_file_path = os.path.join(interim_folder, year)

        ic_sel.to_csv(
            os.path.join(pr_file_path, f"ic_{date_string}.csv"), index=False
        )
        llp.to_csv(
            os.path.join(pr_file_path, f"llp_{date_string}.csv"), index=False
        )
        foreign.to_csv(
            os.path.join(pr_file_path, f"foreign_{date_string}.csv"),
            index=False,
        )

# %%
