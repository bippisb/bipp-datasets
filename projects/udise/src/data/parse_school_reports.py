# %%
import camelot
import pandas as pd
import re
from functools import partial
from get_raw_data import parse_raw_data_path
from pathlib import Path
from typing import Callable
from core import save_json
import traceback

# %%
pd.set_option("display.max_columns", None)

# %%
DATA_DIR = Path(__file__).parents[2] / "data"
RAW_DATA_DIR = DATA_DIR / "raw" / "schools"
DEST_DIR = DATA_DIR / "interim" / "school_reports"
DEST_DIR.mkdir(parents=True, exist_ok=True)
pdf_files = list(RAW_DATA_DIR.rglob("*.pdf"))

# %%


def generate_pattern(keyword):
    return re.compile(rf'.*{re.escape(keyword)}.*', re.IGNORECASE)


def extract_table_between_keywords(table_df: pd.DataFrame, start_keyword, end_keyword=None, get_end_idx: Callable[[pd.Index], int] = lambda i: i.min() - 1) -> pd.DataFrame:
    # Find the index of the row having the start keyword
    start_pattern = generate_pattern(start_keyword)
    start_rows = table_df[table_df.apply(lambda row: bool(
        start_pattern.search(str(row.values))), axis=1)].index
    start_idx = start_rows.min()

    if start_rows.empty:
        return None

    if end_keyword is None:
        return table_df.loc[start_idx:]

    # Find the index of the row having the end keyword
    end_pattern = generate_pattern(end_keyword)
    end_rows = table_df[table_df.apply(
        lambda row: bool(end_pattern.search(str(row.values))),
        axis=1)].index
    end_idx = get_end_idx(end_rows) if not end_rows.empty else len(table_df)

    assert end_idx > start_idx, "End index must be greater than start index"

    return table_df.loc[start_idx:end_idx]

# %%


def read_pdf(file_path: Path, **read_pdf_kwargs):
    fp = file_path.as_posix()
    tables = camelot.read_pdf(fp, **read_pdf_kwargs)
    return tables

# %%


def ffill_character_separated_values(s: pd.Series, interleave_null_values: int = None, split_pattern=r'[\n\s]') -> pd.DataFrame:
    df = s.str.strip().str.split(split_pattern, expand=True, regex=True)
    df = df.stack().reset_index()
    df.index = df["level_0"] + df["level_1"]

    if interleave_null_values is None:
        return df.loc[:, 0].dropna().reset_index(drop=True)

    return pd.Series([
        df.loc[i, 0] if i in df.index else None
        for i in range(interleave_null_values)
    ])


# %%


def parse_enrollment_by_social_category(page_2: pd.DataFrame) -> pd.DataFrame:
    enrollment_table = extract_table_between_keywords(
        page_2, "Enrolment (By Social Category)", "Minority Details")
    df = enrollment_table.replace("", None).reset_index(drop=True)
    df = df.iloc[1:, :-1].reset_index(drop=True)
    df.iloc[0].ffill(inplace=True)
    df = df.T
    assert df.shape == (30, 8)
    df = df.replace("B", "Boys").replace("G", "Girls")
    df = df.replace("Pre-Pr", "Pre-Primary")
    columns = ["class", "gender", "general_category", "sc_category", "st_category",
               "obc_category", "student_count", "total_boys_and_girls_in_class"]
    df.columns = columns
    df = df.iloc[1:].reset_index(drop=True)
    df["total_boys_and_girls_in_class"].ffill(inplace=True)
    return df

# %%


def parse_enrollment_by_minority_group(page_2: pd.DataFrame) -> pd.DataFrame:
    minority_enrol = extract_table_between_keywords(
        page_2, "Minority Details", "Enrolment by grade")
    df = minority_enrol.replace("", None)
    df = df.iloc[1:, :-1].reset_index(drop=True)
    if df.iloc[-1].isnull().all():
        df = df.iloc[:-1].reset_index(drop=True)

    assert df.shape == (13, 30)
    df = df.apply(partial(ffill_character_separated_values,
                  interleave_null_values=30), axis=1)
    columns = ["muslim", "christian", "sikh", "budhist", "parsi", "jain",
               "other", "total", "grand_total", "have_aadhar", "bpl", "repeater", "cwsn"]
    df = df.T
    df.columns = columns
    df = df.iloc[1:].reset_index(drop=True)
    return df

# %%


def parse_enrollment_by_age(page_2: pd.DataFrame) -> pd.DataFrame:
    age_enrol = extract_table_between_keywords(
        page_2, "Enrolment by grade", "Source : UDISE+")

    if age_enrol is None:
        print("No enrollment by age table found.")
        return None
    df = age_enrol.apply(partial(ffill_character_separated_values), axis=1)
    df = df.loc[~df.isnull().all(axis=1)].reset_index(drop=True)
    df = age_enrol.replace("", None).reset_index(drop=True)
    df = df.iloc[3:-1, 0:-1].reset_index(drop=True)
    if df.iloc[-1].isnull().all():
        df = df.iloc[:-1].reset_index(drop=True)
    # Extract all rows starting from row 3, and only column 0
    columns = age_enrol.iloc[3:, 0]
    columns = ffill_character_separated_values(
        columns, interleave_null_values=None)
    columns = columns[columns.str.strip().replace(
        '', pd.NA).notna()].dropna().tolist()
    df = df.T.reset_index(drop=True)
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = columns
    return df

# %%


def parse_page_2(pdf_file: Path) -> pd.DataFrame:
    print(f"Parsing Page 2: {pdf_file}")
    tables = read_pdf(pdf_file, pages="2")
    assert tables.n == 1, "Camelot parses page 2 as a single table"
    page = tables[0].df
    scat_df = parse_enrollment_by_social_category(page)
    minority_df = parse_enrollment_by_minority_group(page)
    age_df = parse_enrollment_by_age(page)
    df = pd.concat([scat_df, minority_df, age_df], axis=1)
    return df

# %%


def parse_teacher_qualifications(page_1: pd.DataFrame) -> pd.DataFrame:
    def get_end_idx(idx: pd.Index):
        return idx.max() + 1
    table = extract_table_between_keywords(
        page_1, "Teacher With Profes", get_end_idx=get_end_idx)
    # Check if the teacher_qual_table is not empty
    if table.empty:
        return None
    df = table.reset_index(drop=True)
    # Extract values using a modified regular expression to split the string
    # Becaue values in the middle two columns are clubbed together in one column
    split_values = df.iloc[1:, 10].str.extract(
        r'(\d+)\s*(\D.*)', expand=True)
    # Assign the correct values to columns
    edu_qual_heads = pd.concat([
        df.iloc[1:-1, 0],
        split_values.iloc[1:-1, 1]
    ], ignore_index=True)
    values = pd.concat([
        split_values.iloc[1:-1, 0],
        df.iloc[1:-1, 19]
    ], ignore_index=True)
    return pd.DataFrame({
        "educational_qualification": edu_qual_heads,
        "no_of_teachers": values
    })

# %%


def parse_enrollment_table_on_page_1_ls_20(roi: pd.DataFrame) -> pd.DataFrame:
    cln = roi.apply(
        partial(ffill_character_separated_values, split_pattern=r'[\n]'),
        axis=1)
    cln.iloc[-1] = cln.iloc[-1].str.strip().replace("", None)
    cln = cln.dropna(how="all")
    assert cln.shape[0] == 3, f"Expected 3 rows, found {cln.shape[0]}"

    def cln_row(s: pd.Series) -> list:
        return s.replace("", None).dropna().to_list()

    classes = cln_row(cln.iloc[0])
    genders = cln_row(cln.iloc[1])
    counts = cln_row(cln.iloc[2])
    assert len(classes) * 2 == len(genders) == len(counts)

    df = pd.DataFrame({
        "gender": genders,
        "count": counts
    })
    df["class"] = [
        _cls
        for _cls in classes
        for _ in range(2)
    ]
    col_order = ["class", "gender", "count"]
    return df[col_order]
# %%


def parse_enrollment_under_rte_section_12(page_1_ls20: pd.DataFrame) -> pd.DataFrame:
    table = extract_table_between_keywords(page_1_ls20, "Total no. of Students Enrolled Under Section 12 of the RTE Act In Private Unaided and Specified Category Schools (DCF Sl. No. 1.42(a))",
                                           "Total no. of Economically Weaker Section*(EWS) students Enrolled in Schools (DCF Sl. No. 1.42(b))")
    df = parse_enrollment_table_on_page_1_ls_20(table.iloc[1:])
    df = df.rename(columns={
        "count": "students_enrolled_under_section_12_of_rte"
    })
    return df

# %%


def parse_enrollment_ews(page_1: pd.DataFrame) -> pd.DataFrame:
    table = extract_table_between_keywords(
        page_1, "Total no. of Economically Weaker Section*(EWS) students Enrolled in Schools (DCF Sl. No. 1.42(b))", "Teachers")
    df = parse_enrollment_table_on_page_1_ls_20(table.iloc[1:])
    df = df.rename(columns={
        "count": "ews_students"
    })
    return df

# %%


def parse_teachers(page_1: pd.DataFrame) -> dict[str, str]:
    table = extract_table_between_keywords(
        page_1, "Teachers", "Teacher With Profes").reset_index(drop=True)
    # split keys and values of classes taught
    split_values = table.iloc[:, 4].str.split(" ", n=1, expand=True)
    table.iloc[2:6, 3] = split_values.iloc[2:6, 0]
    table.iloc[2:6, 4] = split_values.iloc[2:6, 1]
    # split key and value of graduate qualification
    split_values = table.iloc[:, 20].str.split(" ", n=1, expand=True)
    table.iloc[7, [19, 20]] = table.iloc[7, 20].strip().split(" ")
    # split keys and values of gender
    split_values = table.iloc[:, 21].str.split(" ", n=1, expand=True)
    table.iloc[3:6, 20] = split_values.iloc[3:6, 0]
    table.iloc[3:6, 21] = split_values.iloc[3:6, 1]
    # shift keys for nature of appointment towards right
    table.iloc[3:6, 13] = table.iloc[3:6, 12]
    table.iloc[3:6, 12] = None

    interim_df = table

    appointment_and_gender = parse_table_row(
        interim_df,
        column_keywords=["Nature of Appointment", "Gender"],
        n_rows=3,
    )
    if "Nature of Appointment" in appointment_and_gender:
        appointment_and_gender.pop("Nature of Appointment")
    acad_qual = parse_table_row(
        interim_df,
        column_keywords=["Below Graduate"],
        n_rows=3
    )
    graduate_qual = parse_key_value_pairs(interim_df.iloc[7:8, 20:])
    acad_qual = dict(**acad_qual, **graduate_qual)

    classes_taught_1 = parse_table_row(
        interim_df,
        column_keywords=["Primary"],
        n_rows=7
    )
    classes_taught_2 = parse_table_row(
        interim_df,
        column_keywords=["2-Up.Pr."],
        n_rows=4
    )
    classes_taught = dict(**classes_taught_1, **classes_taught_2)
    return dict(**appointment_and_gender, **acad_qual, **classes_taught)

# %%


def get_idx_of_rows_having_keyword(df: pd.DataFrame, keyword: str) -> pd.Index:
    pattern = generate_pattern(keyword)
    def criterion(row): return bool(pattern.search(str(row.values)))
    return df[df.apply(criterion, axis=1)].index
# %%


def get_col_having_keyword(df: pd.DataFrame, keyword: str):
    pattern = generate_pattern(keyword)
    def criterion(row): return bool(pattern.search(str(row.values)))
    return df.loc[:, df.apply(criterion)].columns[0]


# %%

def parse_key_value_pairs(df: pd.DataFrame, nth_val: int = 0) -> dict:
    key_value_dict = {}

    # Iterate over rows in the DataFrame
    for index, row in df.replace("", None).iterrows():
        key = row.iloc[0]  # Assuming the key is in the first column
        key_value_dict[key] = None
        val_seen = 0
        # Iterate over the remaining columns to find the value for the key
        for col_index in range(1, len(row)):
            value = row.iloc[col_index]
            if pd.notna(value):  # Check if the value is not NaN
                if val_seen == nth_val:
                    key_value_dict[key] = value
                    break  # Break the loop once nth non-NaN value is found
                val_seen += 1

    if None in key_value_dict:
        key_value_dict.pop(None)
    return key_value_dict

# %%


def parse_table_row(df: pd.DataFrame, column_keywords: list[str], n_rows: int, nth_col=0, start_col_offset=0) -> dict[str, str]:
    key_columns = [
        get_col_having_keyword(df, keyword)
        for keyword in column_keywords
    ]
    start_idx = get_idx_of_rows_having_keyword(
        df, column_keywords[0]).min()

    data_dict = dict()

    for i, col in enumerate(key_columns):
        end_col = key_columns[i + 1] - \
            1 if i != len(key_columns) - 1 else df.columns[-1]
        roi = df.loc[start_idx:start_idx + n_rows,
                     col + start_col_offset:end_col]
        data = parse_key_value_pairs(roi, nth_val=nth_col)
        data_dict.update(data)

    return data_dict

# %%


def parse_basic_details(page_1_ls20: pd.DataFrame) -> dict[str, str]:
    bsc = parse_table_row(
        page_1_ls20,
        column_keywords=["UDISE CODE", "School Name"],
        n_rows=5
    )

    """
    Block, Pincode, and Municipality are embedded in other values like the following
    'District': 'SOUTH GOA\nMORMUGAO\nBlock',
    'Cluster': 'G.H.S. BAINA',
    'Mohalla': 'BAINA\n403802\nPincode',
    'City': 'VASCO\nMORMUGAO\nMunicipality',
    """

    # parse municipality and clean city value
    bsc["Municipality"] = bsc["City"].split("\n")[-2]
    bsc["City"] = bsc["City"].split("\n")[0]

    # parse block and clean district value
    bsc["Block"] = bsc["District"].split("\n")[-2]
    bsc["District"] = bsc["District"].split("\n")[0]

    # parse pincode
    a = "Mohalla"
    key = a if a in bsc.keys() else "Habitation"
    bsc["Pincode"] = bsc[key].split("\n")[-2]
    bsc[key] = bsc[key].split("\n")[0]

    return bsc


# %%

def parse_things_students_received(page_1_ls20: pd.DataFrame) -> pd.DataFrame:
    keyword = "Students Received"
    parse_row = partial(
        parse_table_row,
        column_keywords=[keyword],
        n_rows=3
    )

    primary = parse_row(page_1_ls20, nth_col=0)
    upper_primary = parse_row(page_1_ls20, nth_col=1)

    for key in primary.keys():
        if keyword in key:
            head_key = key
    primary.pop(head_key)
    upper_primary.pop(head_key)

    df = pd.DataFrame([primary, upper_primary])
    df["group"] = ["Primary", "Upper Primary"]
    return df


# %%

def coalesce_columns(df: pd.DataFrame, keywords: list[str]) -> pd.DataFrame:
    df = df.copy()
    columns = [
        get_col_having_keyword(df, kw)
        for kw in keywords
    ]
    end_col = max(columns)
    operand_cols = set(columns) - {end_col}

    for col in columns:
        df[col] = df[col].str.strip().replace("", None)

    s = df.iloc[:, end_col]
    for col in operand_cols:
        t = df.iloc[:, col]
        s = s.combine_first(t)

    df[end_col] = s
    return df.loc[end_col:]

# %%


def parse_indicators(page_1_ls20: pd.DataFrame) -> pd.DataFrame:
    table = extract_table_between_keywords(
        page_1_ls20, "Indicators", "RTE Act")
    roi = coalesce_columns(table, ["Indicators", "hrs", "CCE"])
    return pd.DataFrame([
        parse_table_row(roi, column_keywords=[
                        "Indicators"], n_rows=4, nth_col=i)
        for i in range(4)
    ])

# %%


def parse_digital_facilities(page_1_ls35: pd.DataFrame) -> dict[str, str]:
    table = extract_table_between_keywords(
        page_1_ls35, "Digital Facilities", "SMC").reset_index(drop=True)

    # split values merged into one cell
    split_values = table.iloc[2:, 6].str.strip(
    ).str.split(" ", n=1, expand=True)
    table.iloc[2:, 5] = split_values.iloc[:, 0]
    table.iloc[2:, 6] = split_values.iloc[:, 1]

    split_values = table.iloc[[1, -1],
                              13].str.strip().str.split(" ", n=1, expand=True)
    table.iloc[[1, -1], 12] = split_values.iloc[:, 0]
    table.iloc[[1, -1], 13] = split_values.iloc[:, 1]

    split_values = table.iloc[-2, 15].strip().split(" ")
    table.iloc[-2, 12] = split_values[0]
    table.iloc[-2, 13] = split_values[1]
    table.iloc[-2, 15] = None

    data = parse_table_row(
        table,
        column_keywords=["ICT", "Internet", "Desktop"],
        n_rows=2,
    )
    return data


# %%


def parse_page_1(pdf_file: Path) -> dict:
    print(f"Parsing Page 1: {pdf_file}")
    tables_ls15 = read_pdf(pdf_file, pages="1")  # default line_scale is 15
    tables_ls20 = read_pdf(pdf_file, pages="1", line_scale=20)
    tables_ls35 = read_pdf(pdf_file, pages="1", line_scale=35)
    assert tables_ls15.n == 1, "Camelot parses page 1 as a single table"
    assert tables_ls20.n == 1, "Camelot parses page 1 as a single table"
    assert tables_ls35.n == 1, "Camelot parses page 1 as a single table"

    # page parsed with line scale 15
    pl15: pd.DataFrame = tables_ls15[0].df

    teacher_qual_df = parse_teacher_qualifications(pl15)

    # page parsed with line scale 20
    pl20: pd.DataFrame = tables_ls20[0].df

    basic_details = parse_basic_details(pl20)

    things_students_received = parse_things_students_received(pl20)
    indicators = parse_indicators(pl20)

    sec12_df = parse_enrollment_under_rte_section_12(pl20)
    ews_df = parse_enrollment_ews(pl20)
    teachers_dict = parse_teachers(pl20)

    tr_1_dict = parse_table_row(
        pl20,
        column_keywords=["School Category",
                         "Medium of Instruction", "Visit of school"],
        n_rows=4
    )
    tr_2_dict = parse_table_row(
        pl20,
        column_keywords=["Year of Establishment",
                         "Is this a Shift School", "Anganwadi At Premises"],
        n_rows=7,
    )

    tr_3_dict = parse_table_row(
        pl20,
        column_keywords=["Total Class Rooms", "Drinking Water Available"],
        n_rows=7,
    )
    facilities = dict(
        **tr_1_dict,
        **tr_2_dict,
        **tr_3_dict
    )

    # page parsed with line scale 35
    pl35: pd.DataFrame = tables_ls35[0].df

    digital_facilites = parse_digital_facilities(pl35)

    return dict(
        basic_details=basic_details,
        teacher_qualifications=teacher_qual_df,
        indicators=indicators,
        rte_section_15_enrollment=sec12_df,
        ews_enrollment=ews_df,
        teachers=teachers_dict,
        facilities=facilities,
        digital_facilites=digital_facilites,
        things_students_received=things_students_received,
    )

# %%


def parse_school_report(pdf_file: Path) -> dict:
    return dict(
        page_1=parse_page_1(pdf_file),
        page_2=parse_page_2(pdf_file),
    )


# %%
def process_school_report(pdf_file: Path) -> None:
    rp = parse_raw_data_path(pdf_file)

    report = parse_school_report(pdf_file)

    dest = DEST_DIR / \
        rp["stateId"] / rp["districtId"] / \
        rp["eduBlockId"] / rp["schoolId"] / pdf_file.stem
    dest.mkdir(parents=True, exist_ok=True)

    # save json data from page 1
    json_data = [
        "basic_details",
        "teachers",
        "facilities",
        "digital_facilites",
    ]
    for item in json_data:
        save_json(report["page_1"][item], dest / f"{item}.json")

    # save dataframes from page 1
    df_data = [
        "indicators",
        "rte_section_15_enrollment",
        "ews_enrollment",
        "things_students_received",
        "teacher_qualifications",
    ]
    for item in df_data:
        report["page_1"][item].to_parquet(dest / f"{item}.parquet")

    # page 2 only contains enrollment details which is parsed as a single dataframe
    report["page_2"].to_parquet(dest / "enrollment_details.parquet")


# %%


def main():
    err = list()
    files = pdf_files[50:]
    for i, file in enumerate(files):
        print(f"Processing {i+1}/{len(pdf_files)}: {file.as_posix()}")
        try:
            process_school_report(file)
        except Exception:
            traceback.print_exc()
            err.append(file)
            print("Skipped", len(err), "files")
    save_json(err, DATA_DIR / "error_files.json")


if __name__ == "__main__":
    main()
    # p = "d:/bippisb/bipp-datasets/projects/udise/data/raw/schools/130/4001/40001/4800019/2018-19.pdf"
    # p = Path(p)
    # process_school_report(p)
