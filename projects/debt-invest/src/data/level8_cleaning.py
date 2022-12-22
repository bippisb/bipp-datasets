from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level8.dta"))

df.drop(
    [
        "FSU_Serial_No",
        "Round",
        "Schedule",
        "Sample",
        "NSS_Region",
        "Stratum",
        "Sub_Stratum",
        "Sub_Round",
        "FOD_Sub_Region",
        "Second_stage_stratum_no",
        "Sample_hhld_No",
        "Visit_number",
        "Level",
        "Filler",
        "Blank",
        "NSC",
    ],
    axis=1,
    inplace=True,
)

var_names = [x.lower() for x in df.columns]
df.columns = var_names

df["animal_category"] = df["serial_no"]


animal_labels = {}
for i in range(1, 18):
    if i <= 6:
        animal_labels[i] = "cattle exotic/cross-bred/descript/non-descript"
        if i <= 2:
            animal_labels[i] += "- young stock"
            if i == 1:
                animal_labels[1] += " (male)"

            else:
                animal_labels[2] += " (female)"
        elif (i > 2) and (i < 6):
            animal_labels[i] += "- female"
            if (i == 3) or (i == 4):
                animal_labels[i] += " breeding cow"
                if i == 3:
                    animal_labels[3] += " (milching)"
                else:
                    animal_labels[4] += ": dry/not calved even once"
            else:
                animal_labels[i] += " other"
        else:
            animal_labels[6] += "- male cattle for work/breeding/other"
    elif (i > 6) and (i < 13):
        animal_labels[i] = "buffalo exotic/cross-bred/descript/non-descript"
        if i <= 8:
            animal_labels[i] += "- young stock"
            if i == 7:
                animal_labels[7] += " (male)"
            else:
                animal_labels[8] += " (female)"
        elif (i > 8) and (i < 12):
            animal_labels[i] += "- female"
            if (i == 9) or (i == 10):
                animal_labels[i] += " breeding buffalo"
                if i == 9:
                    animal_labels[9] += " in milk"
                else:
                    animal_labels[10] += ": dry/not calved even once"
            else:
                animal_labels[i] += " other"
        else:
            animal_labels[12] += "- male for work/breeding/other"


animal_labels[13] = "sub-total_buffalo_cattle"
animal_labels[14] = "ovine and other mammals (sheep, goat, pig, rabbits, etc.)"
animal_labels[
    15
] = "poultry birds (hen, cock, chicken, duck, duckling, other poultry birds, etc.)"
animal_labels[
    16
] = "other including large heads (elephant, camel, horse, mule, pony, donkey, yak, mithun, etc.)"
animal_labels[17] = "total"

df["aninmal_category"] = df["aninmal_category"].map(animal_labels)

csv_path = Path.joinpath(interim_data_path, "level8.csv")
df.to_csv(csv_path, index=False)
