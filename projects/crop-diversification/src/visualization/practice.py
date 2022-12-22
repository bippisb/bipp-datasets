import os

import numpy as np
from styleframe import StyleFrame, utils

path_ag = "./data/interim/agcensus_isb/"
path_nc_var = "ag_census_2015_2016/non_crop_variables_selected.xlsx"

sdf = StyleFrame.read_excel(
    os.path.abspath(path_ag + path_nc_var),
    read_style=True,
    use_openpyxl_styles=False,
)
sdf = sdf.iloc[:, :2]


def only_cells_with_red_text(cell):
    return (
        cell
        if cell.style.font_color in {utils.colors.red, "FFFF0000"}
        else np.nan
    )


sdf_2 = StyleFrame(
    sdf.applymap(only_cells_with_red_text).dropna(axis=0, how="all")
)
print(sdf_2)
