import pandas as pd
from pathlib import Path
import re
import numpy as np

DATA_DIR = Path(__file__).parents[1] / "data"
DEST_PATH = DATA_DIR / "interim" / "ub__last_4_non_tax_revenue_years.csv"



def is_subheading(value, next_value):
    return next_value.startswith(value) and next_value != value

# Function to clean and format raw Excel data
def format_raw_xls_data(xls):
    for df in xls:
        # Remove columns with all NaN values
        cols_to_remove = df.columns[1:3][(df.iloc[:, 1:3].isna().all(axis=0))]
        df.drop(columns=cols_to_remove, inplace=True)

        cols_to_remove = df.columns[1:2][(df.iloc[1:10, 1:2].isna().all(axis=0))]
        df.drop(columns=cols_to_remove, inplace=True)

        # Reset index and remove rows where the next row is a subheading
        df.reset_index(drop=True, inplace=True)

        stack_Sno = []
        stack_value = []

        for i in range(len(df) - 1):
            current_Sno = df.iloc[i, 1]

            if("." in str(current_Sno)):
                next_Sno = df.iloc[i + 1, 1]
                current_value = df.iloc[i,2]

                if pd.notna(current_Sno):
                    while( (len(stack_Sno) != 0 ) and (str(current_Sno).startswith(str(stack_Sno[-1])) is False)  ):
                        stack_Sno.pop()
                        stack_value.pop()

                    stack_Sno.append(current_Sno)
                    stack_value.append(current_value)



                if current_Sno.count('.') >= 3:
                    final_string = current_value+ " " + ' '.join(['({})'.format(element) for element in reversed(stack_value[:-1])])
                    df.iloc[i,2] = final_string

        df.reset_index(drop=True, inplace=True)


def extract_subheads(df):
    subheads=df.loc[df.iloc[:,1].str.strip(".").str.contains(".", na=False, regex=False)].iloc[:,:]
    
    return subheads.iloc[:,1:]


def extract_header(df):
    merged_df = pd.DataFrame(columns=df.columns)
    df.reset_index(drop=True, inplace=True)
    df=df.iloc[1:,:]
    major_head_indices = df[df.iloc[:, 3] == "Major Head"].index  
    first_group = df.iloc[:major_head_indices[0]+1]
    merged_row = first_group.apply(lambda x: ' '.join(x.dropna().astype(str)), axis=0)
    merged_row = merged_row.to_frame().T
    merged_df = pd.concat([merged_df, merged_row], ignore_index=True)
    secgroup=df.iloc[major_head_indices[0]+1:,:]
    df = pd.concat([merged_row, secgroup], ignore_index=True)
    df.iloc[0] = df.iloc[0].str.replace(r'\n', ' ').str.replace(r'crores', ' ', regex=True).str.replace(r'Crores', ' ', regex=True).str.replace(r'In', ' ', regex=True).str.replace(r'\(', ' ', regex=True).str.replace(r'\)', ' ', regex=True)


    return df.iloc[0]


def extract_total_head(df):
    # Assuming the first column is unnamed, you can use iloc[:, 0]
    sdf = df[df.iloc[:, 0].notna()]
    sdf = sdf[sdf.iloc[:, -1].notna()]
    sdf = sdf[sdf.iloc[:, -2].notna()]
    sdf = sdf[sdf.iloc[:, -3].notna()]
    sdf = sdf[sdf.iloc[:, 3].isna()]
    sdf.iloc[:,1]=sdf.iloc[:,0]
    sdf.iloc[:, 0] = sdf.iloc[:, 0].str.replace('Total-', '').str.replace('Net-', '')
    sdf = sdf.drop(sdf.columns[3], axis=1)
    return sdf


def extract_total_h(df):
    sdf = df[df.iloc[:, 1].notna()]
    sdf= sdf[sdf.iloc[:, 1] != "Net"]  
    for i in range(len(sdf) - 1):
            sdf.iloc[i, 0] = sdf.iloc[i-2, 1]  
    sdf = sdf[sdf.iloc[:, 3].isna()]
    sdf = sdf[sdf.iloc[:, -1].notna()]
    sdf = sdf[sdf.iloc[:, -2].notna()]
    sdf = sdf[sdf.iloc[:, -3].notna()]
    sdf = sdf.drop(sdf.columns[3], axis=1)
    sdf.reset_index(drop=True, inplace=True)
    return sdf


# Function to extract heads from a DataFrame
def extract_heads(df):
    numeric_values = pd.to_numeric(df.iloc[:, 0], errors='coerce')
    result_df = df[~numeric_values.isna()]
    return result_df

# Function to map subheadings to heads in a DataFrame
def mapping_subtohead(df):
    heads = extract_heads(df)
    heads = heads.iloc[:, :2]
    heads.columns = ['key', 'head']

    heads['key'] = heads['key'].astype(str).str.split('.').str[0]
    heads['key'] = pd.to_numeric(heads['key'], errors='coerce')
    heads_dict = dict(zip(heads['key'], heads['head']))
    
    subheads = extract_subheads(df)
    total_subhead=extract_total_h(df)
    total_subhead.columns = ['Head', 'subhead'] + list(extract_header(df).iloc[3:])
    total_subhead['Head'] = total_subhead['Head'].astype(str).str.split('.').str[0]
    total_subhead[total_subhead.columns[0]] = pd.to_numeric(total_subhead[total_subhead.columns[0]], errors='coerce')
    subheads.columns = ['Head', 'subhead'] + list(extract_header(df).iloc[3:])
    subheads['Head'] = subheads['Head'].astype(str).str.split('.').str[0]
    subheads[subheads.columns[0]] = pd.to_numeric(subheads[subheads.columns[0]], errors='coerce')
    subheads.columns = total_subhead.columns
    total_head = extract_total_head(df)
    total_head.columns = total_subhead.columns
    total_subhead['Head'] = total_subhead['Head'].map(heads_dict)
    subheads['Head'] = subheads['Head'].map(heads_dict)
    result_df = pd.concat([subheads,total_subhead, total_head])
    return result_df
    # Sort DataFrame by 'Head' column
    # sorted_df = result_df.sort_values(by=['Head'])
    
    # return sorted_df


def split_variable_column(df: pd.DataFrame, column_name: str = "variable"):
    df['year'] = df[column_name].str.extract(r'(\d{4}-\d{4})')
    df['estimate_type'] = df[column_name].str.replace(
        r'\d{4}-\d{4}', '', regex=True).str.strip()
    return df.drop(columns=column_name)


def data_cleaning_pipeline(df: pd.DataFrame):
    return df.pipe(mapping_subtohead).drop(columns='Major Head')

def main():

    files = list((DATA_DIR / "raw" / "non tax revenue").glob("*0.xlsx"))

    xls = [pd.read_excel(f).dropna(axis=1, how="all").dropna(axis=0, how="all") for f in files]

    format_raw_xls_data(xls)

    cln_dfs = map(lambda df: df.pipe(data_cleaning_pipeline).melt(id_vars=['Head','subhead']).pipe(split_variable_column), xls)

    df = pd.concat(cln_dfs)
    col_order = ["year", "estimate_type", "Head","subhead", "value"]
    fnl_df = df[col_order]
    fnl_df['value'] = pd.to_numeric(fnl_df['value'], errors='coerce')

    # Drop rows where "value" column is NaN
    fnl_df = fnl_df.dropna(subset=['value'])
    fnl_df['estimate_type'] = fnl_df['estimate_type'].map(lambda x: str(x).replace('`', '').replace(' ', '').replace('(Incrores)', ''))

    fnl_df.to_csv(DEST_PATH, index=False)

if __name__ == "__main__":
    main()
