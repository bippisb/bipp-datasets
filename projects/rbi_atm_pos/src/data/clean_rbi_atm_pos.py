# %%
# import libraries

from datetime import datetime
from pathlib import Path

import pandas as pd

# folders
project_dir = "../../"
raw_folder = project_dir + "/data/raw/"
rbi_data_folder = raw_folder + "rbi_atm_pos"

# empty list
rbi_atm_pos_list_1 = []
rbi_atm_pos_list_2 = []
# %%
# getting the data
# year_list_1 = ["2014", "2015", "2016", "2017", "2018", "2019"]

# for year in year_list_1:
#     for file_type in [".XLSX", "xls", "xlsx"]:
#         file_list = Path(rbi_data_folder).glob(f"{year}/*.{file_type}")

#         for file in file_list:
#             # file_path = rbi_data_folder + "/rbi_atm_pos_ATM7C0D179A507A4735B4E1343F006DB7CD.XLSX"
#             print(file)
#             data = pd.read_excel(file)

#             # remove rows, columns where no data is present
#             data = data.dropna(how="all", axis=1)
#             data = data.dropna(how="all", axis=0)
#             data = data.replace(r"[^\w\s]|_", "", regex=True)
#             print(data.iloc[0, 0])
#             if data.columns[0] == "Unnamed: 0":
#                 data = data.drop(columns="Unnamed: 0")
#             # get the month and year value from the file
#             month_year = " ".join(data.iloc[0, 0].split(" ")[-2:])
#             print(f"getting the data for {month_year}")
#             # get the rows in which the only the required data is present
#             data_actual = data.iloc[5:55, :].reset_index(drop=True)
#             # drop the sno column in the data
#             data_actual = data_actual.drop(columns="Unnamed: 1")
#             column_names = [
#                 "bank_name",
#                 "atms_onsite",
#                 "atms_offsite",
#                 "pos_online",
#                 "pos_offline",
#                 "cc_outstanding_cards_eom",
#                 "cc_trans_atm",
#                 "cc_trans_pos",
#                 "cc_amt_atm",
#                 "cc_amt_pos",
#                 "dc_outstanding_cards_eom",
#                 "dc_trans_atm",
#                 "dc_trans_pos",
#                 "dc_amt_atm",
#                 "dc_amt_pos",
#             ]
#             print(data_actual.iloc[0, 0:5])
#             data_actual.columns = column_names
#             data_actual["date"] = datetime.strftime(
#                 datetime.strptime(month_year, "%B %Y"), "%m-%Y"
#             )
#             data_actual = data_actual[
#                 [
#                     "date",
#                     "bank_name",
#                     "atms_onsite",
#                     "atms_offsite",
#                     "pos_online",
#                     "pos_offline",
#                     "cc_outstanding_cards_eom",
#                     "cc_trans_atm",
#                     "cc_trans_pos",
#                     "cc_amt_atm",
#                     "cc_amt_pos",
#                     "dc_outstanding_cards_eom",
#                     "dc_trans_atm",
#                     "dc_trans_pos",
#                     "dc_amt_atm",
#                     "dc_amt_pos",
#                 ]
#             ]
#             data_actual["bank_name"] = data_actual["bank_name"].str.title()
#             # convert all numerical columns to respective numeric datatypes
#             for column in data_actual.columns[2:]:
#                 data_actual[column] = pd.to_numeric(data_actual[column])
#             # appending the data to the list
#             rbi_atm_pos_list_1.append(data_actual)

#     # consolidated_data_1 = pd.concat(rbi_atm_pos_list_1).reset_index(drop=True)
# %%
year_list_2 = ["2020", "2021", "2022"]

for year in ["2023"]:
    file_list = Path(rbi_data_folder).glob(f"{year}/*.XLSX")

    for file in file_list:
        # file_path = rbi_data_folder + "/rbi_atm_pos_ATM7C0D179A507A4735B4E1343F006DB7CD.XLSX"
        print(file)
        data = pd.read_excel(file, engine="openpyxl")

        # remove rows, columns where no data is present
        data = data.dropna(how="all", axis=1)
        data = data.dropna(how="all", axis=0)
        
        if data.columns[0] == "Unnamed: 0":
            data = data.drop(columns="Unnamed: 0")
        # get the month and year value from the file
        month_year = " ".join(data.columns[0].split(" ")[-2:])
        print(f"getting the data for {month_year}")

        data = data.iloc[:,1:].reset_index(drop=True)
        # get the rows in which the only the required data is present
        data_actual = data.iloc[5:65, :].reset_index(drop=True)
        # drop the sno column in the data
        # data_actual = data_actual.drop(columns='Unnamed: 1')
        column_names = [
            "bank_name",
            "no_of_onsite_atms",
            "no_of_offsite_atms",
            "no_of_micro_atms",
            "no_of_pos_terminals",
            "no_of_bharat_qr_codes",
            "no_of_upi_qr_codes",
            "no_of_credit_cards",
            "no_of_debit_cards",
            "credit_card_transactions_at_pos_volume",
            "credit_card_transactions_at_pos_value",
            "credit_card_transactions_at_e_com_volume",
            "credit_card_transactions_at_e_com_value",
            "credit_card_transactions_at_others_volume",
            "credit_card_transactions_at_others_value",
            "debit_card_transactions_at_pos_volume",
            "debit_card_transactions_at_pos_value",
            "debit_card_transactions_at_e_com_volume",
            "debit_card_transactions_at_e_com_value",
            "debit_card_transactions_at_others_volume",
            "debit_card_transactions_at_others_value",
            "cash_withdrawl_at_atm_using_credit_card_volume",
            "cash_withdrawl_at_atm_using_credit_card_value",
            "cash_withdrawl_at_atm_using_debit_card_volume",
            "cash_withdrawl_at_atm_using_debit_card_value",
            "cash_withdrawl_at_pos_using_debit_card_volume",
            "cash_withdrawl_at_pos_using_debit_card_value",
        ]
        data_actual.columns = column_names
        data_actual["date"] = datetime.strftime(
            datetime.strptime(month_year, "%B %Y"), "01-%m-%Y"
        )
        data_actual = data_actual[
            [
                "date",
            "bank_name",
            "no_of_onsite_atms",
            "no_of_offsite_atms",
            "no_of_micro_atms",
            "no_of_pos_terminals",
            "no_of_bharat_qr_codes",
            "no_of_upi_qr_codes",
            "no_of_credit_cards",
            "no_of_debit_cards",
            "credit_card_transactions_at_pos_volume",
            "credit_card_transactions_at_pos_value",
            "credit_card_transactions_at_e_com_volume",
            "credit_card_transactions_at_e_com_value",
            "credit_card_transactions_at_others_volume",
            "credit_card_transactions_at_others_value",
            "debit_card_transactions_at_pos_volume",
            "debit_card_transactions_at_pos_value",
            "debit_card_transactions_at_e_com_volume",
            "debit_card_transactions_at_e_com_value",
            "debit_card_transactions_at_others_volume",
            "debit_card_transactions_at_others_value",
            "cash_withdrawl_at_atm_using_credit_card_volume",
            "cash_withdrawl_at_atm_using_credit_card_value",
            "cash_withdrawl_at_atm_using_debit_card_volume",
            "cash_withdrawl_at_atm_using_debit_card_value",
            "cash_withdrawl_at_pos_using_debit_card_volume",
            "cash_withdrawl_at_pos_using_debit_card_value",
            ]
        ]
        data_actual["bank_name"] = data_actual["bank_name"].str.title()
        # convert all numerical columns to respective numeric datatypes
        for column in data_actual.columns[2:]:
            data_actual[column] = pd.to_numeric(data_actual[column])
        # appending the data to the list
        rbi_atm_pos_list_2.append(data_actual)

consolidated_data_2 = pd.concat(rbi_atm_pos_list_2).reset_index(drop=True)

consolidated_data_2.to_csv(f"rbi_atm_pos.csv")


# %%
