import pathlib

import numpy as np
import pandas as pd

# Function to call each csv file and spit out clean csv


def nrega_data_appender(path_name):

    files = list(pathlib.Path(path_name).glob("data/processed/block_lgd_mapped/*.csv"))

    # filtering for test states
    files_new = []
    for file in files:
        if file.stem in (["UTTAR PRADESH", "UTTARAKHAND", "GOA", "PUNJAB", "MANIPUR"]):
            files_new.append(file)

    for file in files_new:
        # Importing TN data
        TN = pd.read_csv(file)
        print(file.stem, " has been loaded.")

        # defining a list of concerend strings to be converted
        columns = [
            "master_work_category_name",
            "work_category_name",
            "work_status",
            "finished_when",
            "is_secure",
        ]

        # Function to convert strings into integers
        def column_mutater(data):
            for column in columns:
                if column == "master_work_category_name":
                    conditions = [
                        data[column] == 1,
                        data[column] == 2,
                        data[column] == 3,
                        data[column] == 4,
                        data[column].isna(),
                    ]
                    options = [
                        "A--PUBLIC WORKS RELATING TO NATURAL RESOURCES MANAGEMENT",
                        "B--INDIVIDUAL ASSETS FOR VULNERABLESECTIONS (ONLY FOR HOUSEHOLDS IN PARAGRAPH 5)",
                        "C--COMMON INFRASTRUCTURE FOR NRLM COMPLIANT SELF HELP GROUPS",
                        "D--RURAL INFRASTUCTURE",
                        np.nan,
                    ]

                elif column == "work_category_name":
                    conditions = [
                        data[column] == 1,
                        data[column] == 2,
                        data[column] == 3,
                        data[column] == 4,
                        data[column] == 5,
                        data[column] == 6,
                        data[column] == 7,
                        data[column] == 8,
                        data[column] == 9,
                        data[column] == 10,
                        data[column] == 11,
                        data[column] == 12,
                        data[column] == 13,
                        data[column] == 14,
                        data[column] == 15,
                        data[column] == 16,
                        data[column].isna(),
                    ]
                    options = [
                        "Anganwadi/Other Rural Infrastructure",
                        "Bharat Nirman Rajeev Gandhi Sewa Kendra",
                        "Coastal Areas",
                        "Drought Proofing",
                        "Fisheries",
                        "Flood Control and Protection",
                        "Food Grain",
                        "Land Development",
                        "Micro Irrigation Works",
                        "Other Works",
                        "Renovation of traditional water bodies",
                        "Rural Connectivity",
                        "Rural Drinking Water",
                        "Rural Sanitation",
                        "Water Conservation and Water Harvesting",
                        "Works on Individuals Land (Category IV)",
                        np.nan,
                    ]

                elif column == "work_status":
                    conditions = [
                        data[column] == 1,
                        data[column] == 2,
                        data[column].isna(),
                    ]
                    options = ["Completed", "Physically Completed", np.nan]

                elif column == "finished_when":
                    conditions = [
                        data[column] == 1,
                        data[column] == 2,
                        data[column] == 3,
                        data[column] == 4,
                        data[column] == 5,
                        data[column].isna(),
                    ]
                    options = [
                        "Finished before start",
                        "On the same day",
                        "After start",
                        "Start date missing",
                        "End date missing",
                        np.nan,
                    ]

                elif column == "is_secure":
                    conditions = [
                        data[column] == 0,
                        data[column] == 1,
                        data[column].isna(),
                    ]
                    options = ["NO", "YES", np.nan]

                data[column] = np.select(conditions, options)

            return data

        TN = column_mutater(TN)
        print(
            "Concerend string columns in", file.stem, "have been converted to integers"
        )

        # Dropping agency name and work type
        TN = TN.drop(["agency_name", "work_type"], axis=1)

        # printing data beofre export
        print("Snapshot of top 25 rows of", file.stem)
        print(TN.head(25))

        # Writing file to the processed data folder
        TN.to_csv(str("data/processed/panchayat_lgd_mapped/") + file.name, index=False)
        print(file.stem, "has been exported as CSV file to processed data directory")

    return print(
        'Looping has ended. Check the directory "data/processed" to find the final state level output files'
    )


nrega_data_appender(".")
