import pathlib

import pandas as pd

nrega_all_states = pd.DataFrame
non_unique_states = []


def nrega_data_appender(path_name):
    files = list(pathlib.Path(path_name).glob("data/raw/NREGA_assets_raw/*.csv"))
    nrega_data_list = []

    for file in files:
        # Cleaning and Importing TN data
        TN = pd.read_csv(file, encoding="ISO8859")

        # ISID
        def isid(
            data, col_names
        ):  # could avoid the col_names and give these cols as default
            blah = data.set_index(col_names)
            if blah.index.is_unique:
                print(
                    str(
                        "The selected columns of "
                        + TN["state"].unique()
                        + " make a unique key"
                    )
                )
            else:
                print(
                    str(
                        "The selected columns of "
                        + TN["state"].unique()
                        + " doesn't make a unique key"
                    )
                )
                non_unique_states.append(data["state"].unique())

        isid(TN, ["block_name", "panchayat_name", "work_code"])

        # TN=TN.set_index(['block_name','panchayat_name','work_code','work_start_fin_year','sanction_amount_in_lakh'])

        # stripping tariling lines in strings
        def trail_strip(data):
            for i in data.select_dtypes(include="object").columns.tolist():
                data[i] = data[i].str.rstrip()
            return data

        TN = trail_strip(TN)

        # removing digits from name strings
        def nrega_string_clean(data):
            for i in ["panchayat_name", "block_name", "district", "state"]:
                if any(data[i].str.contains(r"\d+", regex=True)):
                    data[i] = data[i].str.replace(r"\d+", "")
            return data

        TN = nrega_string_clean(TN)

        # rectifying date coumn types
        def date_cleaner(data):
            for i in ["work_started_date", "work_physically_completed_date"]:
                data[i] = pd.to_datetime(data[i])
            return data

        TN = date_cleaner(TN)

        # appending the states into a list
        nrega_data_list.append(TN)

    # appending the states into rows of a final all states data frame
    # nrega_all_states.append(other=nrega_data_list, ignore_index=True, self=nrega_all_states)
    nrega_all_states = pd.concat(nrega_data_list, axis=0)

    # writing the all states file into csv
    nrega_all_states.to_csv("data/interim/all_states_assets.csv", index=False)

    # return nrega_all_states, non_unique_states


nrega_data_appender(".")
