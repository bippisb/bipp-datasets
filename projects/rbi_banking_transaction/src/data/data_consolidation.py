import math
import os
from datetime import datetime

import numpy as np
import pandas as pd

neft_df = pd.DataFrame()
internet_banking_df = pd.DataFrame()
rtgs_df = pd.DataFrame()
mobile_banking_df = pd.DataFrame()
ecs_df = pd.DataFrame()

months = {
    "Jan": "January",
    "Feb": "February",
    "Mar": "March",
    "Apr": "April",
    "Jun": "June",
    "Jul": "July",
    "Aug": "August",
    "Sept": "September",
    "Oct": "October",
    "Nov": "November",
    "Dec": "December",
}

neft_cols = [
    "s_no",
    "bank_name",
    "no_of_debit_transactions",
    "amt_of_debit_transactions",
    "no_of_credit_transactions",
    "amt_of_credit_transactions",
]

internet_banking_col = [
    "s_no",
    "bank_name",
    "no_of_transactions",
    "amt_of_transactions",
    "active_users",
]

rtgs_cols = [
    "s_no",
    "bank_name",
    "inward_interbank_volume",
    "inward_costumer_volume",
    "inward_total_volume",
    "inward_percentage_volume",
    "inward_interbank_amt",
    "inward_costumer_amt",
    "inward_total_amt",
    "inward_percentage_amt",
    "outward_interbank_volume",
    "outward_costumer_volume",
    "outward_total_volume",
    "outward_percentage_volume",
    "outward_interbank_amt",
    "outward_costumer_amt",
    "outward_total_amt",
    "outward_percentage_amt",
]

mobile_banking_col = [
    "s_no",
    "bank_name",
    "no_of_transactions",
    "amt_of_transactions",
    "no_of_active_costumers",
]

ecs_col = [
    "s_no",
    "center_name",
    "bank_name",
    "no_of_credit_users",
    "no_of_credit_transactions",
    "amt_of_credit_transactions",
    "no_of_debit_users",
    "no_of_debit_transactions",
    "amt_of_debit_transactions",
]


def clean_rows(df, total_cols):
    remove_idx = []
    for i in range(len(df)):
        count = 0
        arr = np.array(df.iloc[i])
        for data in arr:
            try:
                if not math.isnan(data) and data != " ":
                    count += 1
            except TypeError:
                if data != " ":
                    count += 1
        if count < total_cols:
            remove_idx.append(i)
    return df.drop(remove_idx)


def clean_cols(df):
    remove_idx = []
    for i, col in enumerate(df.columns):
        count = 0
        arr = np.array(df[col])
        for data in arr:
            try:
                if not math.isnan(data) and data != " ":
                    count += 1
            except TypeError:
                if data != " ":
                    count += 1
        if count < 35:
            remove_idx.append(i)
    return df.drop(df.columns[remove_idx], axis=1)


def clean_data(df, category):
    df = clean_cols(df)
    total_cols = len(df.columns)
    df = clean_rows(df, total_cols)
    if category == "neft":
        if total_cols == 5:
            df.columns = neft_cols[1:]
        else:
            df.columns = neft_cols
            df = df.drop(columns=["s_no"])
    elif category == "internet_banking":
        df.columns = internet_banking_col
        df = df.drop(columns=["s_no"])
    elif category == "rtgs":
        if total_cols == 17:
            df.columns = rtgs_cols[1:]
        else:
            df.columns = rtgs_cols
            df = df.drop(columns=["s_no"])
    elif category == "mobile_banking":
        if total_cols == 4:
            df.columns = mobile_banking_col[0:-1]
        else:
            df.columns = mobile_banking_col
        df = df.drop(columns=["s_no"])
    elif category == "ecs" and total_cols == 9:
        df.columns = ecs_col
        df = df.drop(columns=["s_no"])
    return df


def format(str):
    temp_str = ""
    for i, char in enumerate(str):
        temp_str += char.upper() if i == 0 else char.lower()
    return temp_str


def get_month(df, file_path):
    for col in df.columns:
        if col.split(":")[0] != "Unnamed":
            try:
                data = col
                if data.split(" ")[-2] == "for" or data.split(" ")[-2] == "of":
                    return format(data.split(" ")[-1].split("-")[0])
                data = data.split(" ")[-2]
                data = data if (data[-1] != "," and data[-1] != "-") else data[0:-1]
                return format(data)
            except IndexError:
                print(file_path)
    for i in range(len(df)):
        for col in df.columns:
            data = df[col].iloc[i]
            try:
                if not math.isnan(data) and data != " ":
                    if data.split(" ")[-2] == "for" or data.split(" ")[-2] == "of":
                        return format(data.split(" ")[-1].split("-")[0])
                    data = data.split(" ")[-2]
                    data = data if (data[-1] != "," and data[-1] != "-") else data[0:-1]
                    return format(data)
            except TypeError:
                try:
                    if data != " ":
                        if data.split(" ")[-2] == "for" or data.split(" ")[-2] == "of":
                            return format(data.split(" ")[-1].split("-")[0])
                        data = data.split(" ")[-2]
                        data = (
                            data
                            if (data[-1] != "," and data[-1] != "-")
                            else data[0:-1]
                        )
                        return format(data)
                except IndexError:
                    print(file_path)
            except IndexError:
                print(file_path)


units = {
    "lakh": 1,
    "crore": 100,
    "million": 10,
    "billion": 10000,
    "rs.'000": 0.01,
    "thousand": 0.01,
    "rs'000": 0.01,
}


def get_factor(df1):
    for i in range(0, 15):
        for col in df1.columns:
            data = df1[col].iloc[i]
            try:
                math.isnan(data)
            except TypeError:
                data = data.lower()
                for unit in units.keys():
                    if unit in data:
                        return units[unit]
    return 1


months_val = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}

root_dir = "data/raw/"

for year in os.listdir(root_dir):
    if year == ".gitkeep":
        continue
    year_path = os.path.join(root_dir, year)
    print(year)
    for file in os.listdir(year_path):
        file_path = os.path.join(year_path, file)
        df = pd.ExcelFile(file_path)
        for i, sheet in enumerate(df.sheet_names):
            data = sheet.split(" ")
            for text in data:
                temp_text = text.lower().strip()
                if (
                    temp_text == "rtgs"
                    or temp_text == "ecs"
                    or temp_text == "neft"
                    or temp_text == "mobile"
                    or temp_text == "internet"
                ):
                    if temp_text == "mobile" or temp_text == "internet":
                        temp_text += "_banking"
                    dest_path = "data/interim/datasets"
                    if (
                        temp_text == "neft"
                        or temp_text == "internet_banking"
                        or temp_text == "rtgs"
                        or (temp_text == "mobile_banking" and int(year) >= 2013)
                        or (temp_text == "ecs" and int(year) >= 2013)
                    ):
                        df1 = pd.read_excel(file_path, sheet_name=sheet)
                        factor = get_factor(df1)
                        month = get_month(df1, file_path)
                        if (
                            file == "RTGS02162AA6BC5EEF0541EBADD2404A59F27BE3.XLS"
                            and int(year) == 2016
                        ):
                            month = "February"
                        elif (
                            file == "RTGS100516F68EAB6BB7F94A15B0D283EF24AE87C1.XLS"
                            and int(year) == 2016
                        ):
                            month = "March"
                        if month in months.keys():
                            month = months[month]
                        if month.strip() == "":
                            print(file_path)
                        if month not in months.values() and month != "May":
                            print(month)
                        df1 = clean_data(df1, temp_text)
                        date = datetime(int(year), months_val[month], 1)
                        date = date.strftime("%d-%m-%Y")
                        date_arr = [date for i in range(len(df1))]
                        df1.reset_index(inplace=True)
                        df1 = df1.drop(columns=["index"])
                        df2 = pd.DataFrame(date_arr, columns=["date"])
                        df1 = pd.concat([df2, df1], axis=1)
                        for col in df1.columns:
                            if col.split("_")[0] == "amt" or (
                                col.split("_")[-1] == "amt"
                                and col.split("_")[-2] != "percentage"
                            ):
                                for i in range(len(df1)):
                                    try:
                                        df1.at[i, col] = df1[col].iloc[i] * factor
                                    except TypeError:
                                        continue
                        if "bank_name" in df1.columns:
                            for i in range(len(df1)):
                                data = df1["bank_name"].iloc[i]
                                data = data.split(" ")
                                for idx in range(len(data)):
                                    data[idx] = format(data[idx])
                                df1.at[i, "bank_name"] = " ".join(data)
                        if temp_text == "neft":
                            if len(neft_df):
                                neft_df = pd.concat([neft_df, df1], ignore_index=True)
                            else:
                                neft_df = df1
                        elif temp_text == "internet_banking":
                            df1 = df1.iloc[1:, :]
                            if len(internet_banking_df):
                                internet_banking_df = pd.concat(
                                    [internet_banking_df, df1], ignore_index=True
                                )
                            else:
                                internet_banking_df = df1
                        elif temp_text == "rtgs":
                            if len(rtgs_df):
                                rtgs_df = pd.concat([rtgs_df, df1], ignore_index=True)
                            else:
                                rtgs_df = df1
                        elif temp_text == "mobile_banking":
                            df1 = df1.iloc[1:, :]
                            if len(mobile_banking_df):
                                mobile_banking_df = pd.concat(
                                    [mobile_banking_df, df1], ignore_index=True
                                )
                            else:
                                mobile_banking_df = df1
                        elif temp_text == "ecs" and len(df1.columns) == 9:
                            for i in range(len(df1)):
                                data = df1["center_name"].iloc[i]
                                data = data.split(" ")
                                for idx in range(len(data)):
                                    data[idx] = format(data[idx])
                                df1.at[i, "center_name"] = " ".join(data)
                            if len(ecs_df):
                                ecs_df = pd.concat([ecs_df, df1], ignore_index=True)
                            else:
                                ecs_df = df1
                    break

for i in range(len(mobile_banking_df)):
    data = mobile_banking_df["no_of_active_costumers"].iloc[i]
    if math.isnan(data):
        mobile_banking_df.at[i, "no_of_active_costumers"] = 0

neft_df.to_csv("data/processed/neft_final_file.csv", index=False, encoding="utf8")
internet_banking_df.to_csv(
    "data/processed/internet_banking_final_file.csv", index=False, encoding="utf8"
)
rtgs_df.to_csv("data/processed/rtgs_final_file.csv", index=False, encoding="utf8")
mobile_banking_df.to_csv(
    "data/processed/mobile_banking_final_file.csv", index=False, encoding="utf8"
)
ecs_df.to_csv("data/processed/ecs_final_file.csv", index=False, encoding="utf8")
