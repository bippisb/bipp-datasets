# %%
import camelot
import pandas as pd
import re
from functools import partial
from random import choice
from pathlib import Path
import numpy as np
# %%
pd.set_option("display.max_columns", None)

# %%
DATA_DIR = Path(__file__).parents[1] / "data"
DIR = DATA_DIR / "raw" / "schools"
pdf_files = list(DIR.rglob("*.pdf"))

# %%    


def generate_pattern(keyword):
    return re.compile(rf'.*{re.escape(keyword)}.*', re.IGNORECASE)


def extract_table_between_keywords(table_df: pd.DataFrame, start_keyword, end_keyword):
    start_pattern = generate_pattern(start_keyword)
    end_pattern = generate_pattern(end_keyword)

    # Find the row numbers of start and end keywords using regex
    start_rows = table_df[table_df.apply(lambda row: bool(
        start_pattern.search(str(row.values))), axis=1)].index
    end_rows = table_df[table_df.apply(lambda row: bool(
        end_pattern.search(str(row.values))), axis=1)].index

    if start_rows.empty:
        return None

    # get row indexes of start and end keywords
    start_idx = start_rows.min()
    end_idx = end_rows.min() - 1 if not end_rows.empty else len(table_df)
    assert end_idx > start_idx, "End index must be greater than start index"

    return table_df.loc[start_idx:end_idx]


# %%
def parse_table(file_path: Path, line_scale=15):
    fp = file_path.as_posix()
    tables = camelot.read_pdf(fp, flavor='lattice', pages='all', line_scale=line_scale)
    assert tables.n == 2
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
def parse_enrollment_by_social_category(page_2: pd.DataFrame):
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


def parse_enrollment_by_minority_group(page_2: pd.DataFrame):
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

def extract_and_parse_teacher_qual(page_1: pd.DataFrame):
    teacher_qual_table = extract_table_between_keywords(
        page_1, "Teacher With Profes", "Diploma/degree in special Educatio"
    )
    # Check if the teacher_qual_table is not empty
    if teacher_qual_table.empty:
        return None
    # Reset index for proper alignment
    teacher_qual_table.reset_index(drop=True, inplace=True)
    # Extract values using a modified regular expression to split the string
    split_values = teacher_qual_table.iloc[1:, 10].str.extract(
        r'(\d+)\s*(\D.*)', expand=True)
    # Assign the correct values to columns
    teacher_qual_table.loc[1:, 1] = split_values[0]
    teacher_qual_table.loc[1:, 2] = split_values[1]
    final_table = pd.concat([teacher_qual_table[0], split_values[0],
                            split_values[1], teacher_qual_table[19]], axis=1)
    final_table = final_table.iloc[1:-1].reset_index(drop=True)
    final_table.columns = [0, 1, 2, 3]
    return final_table

def students_enrolled_under_section_12(page_1: pd.DataFrame):
    table = extract_table_between_keywords(page_1, "Total no. of Students Enrolled Under Section 12 of the RTE Act In Private Unaided and Specified Category Schools (DCF Sl. No. 1.42(a))", "Total no. of Economically Weaker Section*(EWS) students Enrolled in Schools (DCF Sl. No. 1.42(b))")
    table = table.apply(partial(ffill_character_separated_values, interleave_null_values=None), axis=1)
    table = table.T.reset_index(drop=True)
    table = table.iloc[:, 3:]
    table.replace('', np.nan, inplace=True)
    table = table.dropna(how='all') 
    table = table.dropna(how='all', axis=1)  
    table = table.reset_index(drop=True)
    table.columns=["students_enrolled_under_section_1"]
    return table

def  economically_weaker(page_1: pd.DataFrame):
    table = extract_table_between_keywords(page_1,"Total no. of Economically Weaker Section*(EWS) students Enrolled in Schools (DCF Sl. No. 1.42(b))", "Teachers")
    table = table.apply(partial(ffill_character_separated_values, interleave_null_values=None), axis=1)
    table = table.T.reset_index(drop=True)
    table = table.iloc[:, 3:]
    table.replace('', np.nan, inplace=True)
    table = table.dropna()
    table = table.reset_index(drop=True)
    table.columns=["economically_weaker"]
    return table

def teachers(page_1: pd.DataFrame):
    table = extract_table_between_keywords(page_1, "Teachers", "Teacher With Profes")
    split_values = table.iloc[:, 4].str.split(" ", n=1, expand=True)
    table.iloc[:, 3] = split_values[0]
    table.iloc[:, 4] = split_values.iloc[:, 1]
    split_values = table.iloc[:, 21].str.split(" ", n=1, expand=True)
    table.iloc[:, 20] = split_values[0]
    table.iloc[:, 21] = split_values.iloc[:, 1]
    value_to_cut = table.iloc[6, 2]
    table.iloc[6, 2] = ' ' 
    table.iloc[6, 3] = value_to_cut
    return table


# %%


def parse_page_2(pdf_file: Path, line_scale=15):
    print(f"parsing {pdf_file}")
    tables = parse_table(pdf_file, line_scale=line_scale)
    t1, t2 = list(map(lambda t: t.df, tables))
    scat = parse_enrollment_by_social_category(t2)
    minor = parse_enrollment_by_minority_group(t2)
    age = parse_enrollment_by_age(t2)
    if age is None:
        print("No enrollment by age table found.")
    df = pd.concat([scat, minor, age], axis=1)
    return df

def test_parse_page_2_on_all_files():
    for pdf_file in DIR.rglob("*.pdf"):
        parse_page_2(pdf_file)

# %%


def parse_page_1(pdf_file: Path):
    tables = parse_table(pdf_file)
    t1, t2 = list(map(lambda t: t.df, tables))
    df = extract_and_parse_teacher_qual(
        t1, "Teacher With Profes", "Diploma/degree in special Educatio")
    return df


# %%


def get_idx_of_rows_having_keyword(df: pd.DataFrame, keyword: str):
    pattern = generate_pattern(keyword)
    def criterion(row): return bool(pattern.search(str(row.values)))
    return df[df.apply(criterion, axis=1)].index
# %%


def get_col_having_keyword(df: pd.DataFrame, keyword: str):
    pattern = generate_pattern(keyword)
    def criterion(row): return bool(pattern.search(str(row.values)))
    return df.loc[:, df.apply(criterion)].columns[0]


# %%

def parse_key_value_pairs(df: pd.DataFrame):
    key_value_dict = {}

    # Iterate over rows in the DataFrame
    for index, row in df.replace("", None).iterrows():
        key = row.iloc[0]  # Assuming the key is in the first column
        key_value_dict[key] = None
        # Iterate over the remaining columns to find the value for the key
        for col_index in range(1, len(row)):
            value = row.iloc[col_index]
            if pd.notna(value):  # Check if the value is not NaN
                key_value_dict[key] = value
                break  # Break the loop once a non-NaN value is found

    return key_value_dict

# %%


def parse_table_row(df: pd.DataFrame, column_keywords: list[str], n_rows: int) -> dict[str, str]:
    key_columns = [
        get_col_having_keyword(df, keyword)
        for keyword in column_keywords
    ]
    start_idx = get_idx_of_rows_having_keyword(
        t1, column_keywords[0]).min()

    data_dict = dict()

    for i, col in enumerate(key_columns):
        end_col = key_columns[i + 1] - \
            1 if i != len(key_columns) - 1 else df.columns[-1]
        print(end_col)
        roi = df.loc[start_idx:start_idx + n_rows, col:end_col]
        print(roi)
        data = parse_key_value_pairs(roi)
        data_dict.update(data)

    return data_dict


# %%
infra_rows = [
    dict(
        column_keywords=["School Category",
                         "Medium of Instruction", "Visit of school"],
        n_rows=4
    ),
    dict(
        column_keywords=["Year of Establishment",
                         "Is this a Shift School", "Anganwadi At Premises"],
        n_rows=7
    ),
    dict(
        column_keywords=["Total Class Rooms", "Drinking Water Available"],
        n_rows=7
    )
]

# %%
fp = pdf_files[0]
print(fp)
tables1 = camelot.read_pdf(str(fp), pages='1', line_scale=20)
t1 = tables1[0].df
tables2 = camelot.read_pdf(str(fp), pages='2', line_scale=15)
t2=tables2[0].df
tables3 = camelot.read_pdf(str(fp), pages='1', line_scale=15)
t3 = tables3[0].df
scat = parse_enrollment_by_social_category(t2)
minor = parse_enrollment_by_minority_group(t2)
age = parse_enrollment_by_age(t2)
df = pd.concat([scat, minor,age], axis=1)
print(df)
students_section_12=students_enrolled_under_section_12(t1)
print(students_section_12)
students_economically_weaker=economically_weaker(t1)
print(students_economically_weaker)
teachers_qual=extract_and_parse_teacher_qual(t3,"Teacher With Profes","Diploma/degree in special Educatio")
print(teachers_qual)
teachers_table=teachers(t1)
print(teachers_table)
df = pd.concat([scat, minor,age,students_section_12,students_economically_weaker],axis=1)
print(df)
