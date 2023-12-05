# %%
import camelot
import pandas as pd
import re
from functools import partial
from random import choice
from pathlib import Path

# %%
pd.set_option("display.max_columns", None)

# %%
DIR = Path(__file__).parents[1] / "data" / "raw" / "schools"
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
def parse_table(file_path: Path):
    fp = file_path.as_posix()
    tables = camelot.read_pdf(fp, flavor='lattice', pages='all')
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
    df = df.apply(partial(ffill_character_separated_values, interleave_null_values=30), axis=1)
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

    df = age_enrol.replace("", None).reset_index(drop=True)
    df = df.iloc[3:-1, 1:-1].reset_index(drop=True)

    if df.iloc[-1].isnull().all():
        df = df.iloc[:-1].reset_index(drop=True)

    # Ensure the DataFrame has 24 rows
    if df.shape[0] < 24:
        # Add None rows at the beginning to make it 24 rows
        none_rows = pd.DataFrame(
            [[None] * df.shape[1]] * (24 - df.shape[0]), columns=df.columns)
        df = pd.concat([none_rows, df], ignore_index=True)
    columns=ffill_character_separated_values(df)
    print(columns)

    #columns = ['age_gt_3', 'age_4', 'age_lt_5', 'age_5', 'age_6', 'age_7', 'age_8', 'age_9', 'age_10', 'age_11', 'age_12', 'age_13',
               #age_14', 'age_15', 'age_16', 'age_17', 'age_18', 'age_19', 'age_20', 'age_21', 'age_22', 'age_gt_22', 'total']
    df = df.T.reset_index(drop=True)
    df.columns = columns
    return df

def extract_and_parse_teacher_qual(page_1: pd.DataFrame, start_keyword, end_keyword):
    start_pattern = generate_pattern(start_keyword)
    end_pattern = generate_pattern(end_keyword)
    # Find the row numbers of start and end keywords using regex
    start_rows = page_1[page_1.apply(lambda row: bool(
        start_pattern.search(str(row.values))), axis=1)].index
    end_rows = page_1[page_1.apply(lambda row: bool(
        end_pattern.search(str(row.values))), axis=1)].index
    if start_rows.empty:
        return None
    # get row indexes of start and end keywords
    start_idx = start_rows.min()
    end_idx = end_rows.max() + 1 if not end_rows.empty else len(page_1)
    assert end_idx > start_idx, "End index must be greater than start index"
    teacher_qual_table = page_1.loc[start_idx:end_idx].copy()  # Create a copy to avoid the SettingWithCopyWarning
    # Check if the teacher_qual_table is not empty
    if teacher_qual_table.empty:
        return None
    # Reset index for proper alignment
    teacher_qual_table.reset_index(drop=True, inplace=True)
    # Extract values using a modified regular expression to split the string 
    split_values = teacher_qual_table.iloc[1:, 10].str.extract(r'(\d+)\s*(\D.*)', expand=True)
    # Assign the correct values to columns
    teacher_qual_table.loc[1:, 1] = split_values[0]
    teacher_qual_table.loc[1:, 2] = split_values[1]
    final_table = pd.concat([teacher_qual_table[0], split_values[0], split_values[1], teacher_qual_table[19]], axis=1)
    final_table = final_table.iloc[1:-1].reset_index(drop=True)
    final_table.columns = [0, 1, 2, 3]
    return final_table

def economically_weak(page_1: pd.DataFrame):
    table = extract_table_between_keywords(
        page_1, "Total no. of Economically Weaker Section*(EWS) students Enrolled in Schools (DCF Sl. No. 1.42(b)", "Classes Taught")
    df = table.replace("", None).reset_index(drop=True)
    df = df.iloc[1:, :-1].reset_index(drop=True)
    df.iloc[0].ffill(inplace=True)
    df = df.replace("B", "Boys").replace("G", "Girls")
    df = df.replace("Pre-Pr", "Pre-Primary")
    print(df)
    return df



# %%
pdf_file = choice(pdf_files)
print(pdf_file.as_posix())
tables = parse_table(pdf_file)

# %%
t1, t2 = list(map(lambda t: t.df, tables))


df2=extract_and_parse_teacher_qual(t1,"Teacher With Profes","Diploma/degree in special Educatio")



scat = parse_enrollment_by_social_category(t2)
minor = parse_enrollment_by_minority_group(t2)
age = parse_enrollment_by_age(t2)
print(age)

# %%
df = pd.concat([scat, minor, age], axis=1)
# %%
