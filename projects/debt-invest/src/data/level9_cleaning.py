from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")

df = pd.read_stata(Path.joinpath(raw_data_path, "level9.dta"))

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

df["transport_category"] = df["serial_no"]

transport_labels = {
    1: "tractors (all types)",
    2: "motor cars/jeep/van",
    3: "motorcycles/ scooters/ mopeds/ auto-rickshaws",
    4: "rickshaw/e-rickshaw/toto rickshaw/van rickshaw",
    5: "bicycles",
    6: "carts (hand-driven / animal driven)",
    7: "other transport equipment incl. boats, trucks,trailers, light commercial vehicles (LCV), passenger buses, etc.",
    8: "total",
}

use_labels = {
    1: "for farm business",
    2: "for non-farm business",
    3: "for household use",
}

df["transport_category"] = df["transport_category"].map(transport_labels)
df["equipment_owned"] = df["equipment_owned"].map(use_labels)

csv_path = Path.joinpath(interim_data_path, "level9.csv")
df.to_csv(csv_path, index=False)
