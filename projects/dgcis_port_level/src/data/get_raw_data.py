import requests
import pandas as pd
from datetime import datetime, timedelta
import warnings

# Ignoring a specific warning by category
warnings.filterwarnings("ignore")

# Function for URL request
def url_response(formatted_date, total_records):
    """ In reqUrl, eximp=E is for export, and need to change it for import.
        Similarly outputfolder is for export, and need to change it for import."""
    
    reqUrl = f"https://119.226.26.106/dgcis/freeusersearch?eximp=E&datepicker={formatted_date}&datepicker1={formatted_date}&commodities=A&countries=A&type=10&ports=A&sorted=Order%20By%20HS_CODE,CTY,Value%20DESC&currency=B&reg=2&offset=1&noOfRecords={total_records}&scrollDate=AUG23"
    headersList = {
        "Accept": "*/*",
        "User-Agent": "Thunder Client (https://www.thunderclient.com)"
    }
    payload = ""
    response = requests.post(reqUrl, data=payload, headers=headersList, verify=False)
    return response

# Function to get the total number of records
def get_total_records(response, formatted_date):

    total_records = 10
    response = url_response(formatted_date, total_records)
    response_data = response.json()
    total_records = response_data['totalNoOfRecords']
    print(f"Total records for {formatted_date}: {total_records}")
    return total_records

# Function to make a request and save dataframe to CSV
def make_request_and_save_csv(formatted_date, total_records):

    response_2 = url_response(formatted_date, total_records)

    # Check the request status
    if response_2.status_code == 200:
        # Convert the 'searchlists' field to a list of dictionaries
        response_data = response_2.json()
        search_lists = response_data['searchlists']

        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(search_lists)

        # Add a 'month' column to the dataframe
        df['month'] = formatted_date

        # Save the dataframe to a CSV file
        csv_filename = f"export_data_{formatted_date}.csv"
        df.to_csv(f"../data/raw/export/{csv_filename}", index=False)

        print(f"Data for {formatted_date} saved to {csv_filename}")
    else:
        print(f"Failed to retrieve data for {formatted_date}. Status code: {response_2.status_code}")

# Get the current year and month
current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month

# Loop over the last 5 years and all months
for year in range(current_year - 5, current_year + 1):
    for month in range(1, 13):
        # Skip future months
        if year == current_year and month > current_month:
            continue
        formatted_date = datetime(year, month, 1).strftime("%b-%Y")
        print(f"Retrieving data for {formatted_date}")
        total_records = get_total_records(url_response, formatted_date)
        make_request_and_save_csv(formatted_date, total_records)