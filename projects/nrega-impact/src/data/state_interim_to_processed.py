import pathlib

import numpy as np
import pandas as pd

# Function to call each csv file and spit out clean csv


def nrega_data_appender(path_name):

    files = list(pathlib.Path(path_name).glob("data/interim/NREGA_assets/*.csv"))

    for file in files:
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
                        data[column]
                        == "A--PUBLIC WORKS RELATING TO NATURAL RESOURCES MANAGEMENT",
                        data[column]
                        == "B--INDIVIDUAL ASSETS FOR VULNERABLESECTIONS (ONLY FOR HOUSEHOLDS IN PARAGRAPH 5)",
                        data[column]
                        == "C--COMMON INFRASTRUCTURE FOR NRLM COMPLIANT SELF HELP GROUPS",
                        data[column] == "D--RURAL INFRASTUCTURE",
                        data[column].isna(),
                    ]
                    options = [1, 2, 3, 4, np.nan]

                elif column == "work_category_name":
                    conditions = [
                        data[column] == "Anganwadi/Other Rural Infrastructure",
                        data[column] == "Bharat Nirman Rajeev Gandhi Sewa Kendra",
                        data[column] == "Coastal Areas",
                        data[column] == "Drought Proofing",
                        data[column] == "Fisheries",
                        data[column] == "Flood Control and Protection",
                        data[column] == "Food Grain",
                        data[column] == "Land Development",
                        data[column] == "Micro Irrigation Works",
                        data[column] == "Other Works",
                        data[column] == "Renovation of traditional water bodies",
                        data[column] == "Rural Connectivity",
                        data[column] == "Rural Drinking Water",
                        data[column] == "Rural Sanitation",
                        data[column] == "Water Conservation and Water Harvesting",
                        data[column] == "Works on Individuals Land (Category IV)",
                        data[column].isna(),
                    ]
                    options = list(range(1, 17))
                    options.append(np.nan)

                elif column == "work_status":
                    conditions = [
                        data[column] == "Completed",
                        data[column] == "Physically Completed",
                        data[column].isna(),
                    ]
                    options = [1, 2, np.nan]

                elif column == "finished_when":
                    conditions = [
                        data[column] == "Finished before start",
                        data[column] == "On the same day",
                        data[column] == "After start",
                        data[column] == "Start date missing",
                        data[column] == "End date missing",
                        data[column].isna(),
                    ]
                    options = [1, 2, 3, 4, 5, np.nan]

                elif column == "is_secure":
                    conditions = [
                        data[column] == "NO",
                        data[column] == "YES",
                        data[column].isna(),
                    ]
                    options = [0, 1, np.nan]

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
        TN.to_csv(str("data/processed/") + file.name, index=False)
        print(file.stem, "has been exported as CSV file to processed data directory")

    return print(
        'Looping has ended. Check the directory "data/processed" to find the final state level output files'
    )


nrega_data_appender(".")
