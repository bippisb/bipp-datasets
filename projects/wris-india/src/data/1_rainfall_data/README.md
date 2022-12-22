# Scraping Scripts

This folder has the script which is used to download the rainfall data from the [India WRIS](https://indiawris.gov.in/wris/#/DataDownload) website for Rainfall, at station level and daily values since 1970 till date.


Script will be written in such a way that user can input the start date and end date parameters and the data for that period will be downloaded as excel files.

    Note: This script needs to be modified for other datasets with respect to request payloads being sent for api calls.

Two scripts were created, one using scrapy(working script) and the other using selenium.

Downloaded data will be written to data/raw folder in this project. Scripts will be written to automate the upload to a sharepoint drive. To run the scraper this below is the command, from the project directory.

    poetry run python .\src\data\1_rainfall_data\get_raw_data_scrapy.py

### Working Logic for the Scraper
1. First Requests are sent to the [base API](https://wdo.indiawris.gov.in/api/comm/src/rainfall_report) which is the starting point to get the data.
2. Next step is to get the [Locations API](https://wdo.indiawris.gov.in/api/locations/allChildrenForParentChildType) with the necessary payloads as POST requests and save all the state level, district level and station level data as global variables to access them at any opint
3. Make an iterator to go through each state, each district, each station and then to iterate over the years.
4. Website doesnt allow start date and end date to be more than one year apart. If we start with 01-jan-1970 the end date has to be 01-jan-1971 or less.
5. Creating a logic to get the user input while invoking the script to input start year and end year. If the start year and end year are same as current year, it will ask the user to input the start date in a specific format and takes the end date as the previous day to the day in which script is run. 
6. If the start year and end year are not same as current year, then it will iterate from starting of each year to end of the year getting the data.

The data will be stored in data/raw folder of the project which will be added to Raw Data in the IDP SharePoint drive, manually for now.