# import libraries
import pandas as pd
import os

raw_path = 'data/raw'
years = ['2015','2016','2017','2018','2019','2020','2021']

# consolidate the data of Andaman and Nicobar Islands seperately
if not os.path.isdir('data/interim/Andaman and Nicobar Islands'):
    os.mkdir('data/interim/Andaman and Nicobar Islands')

if not os.path.isfile('data/interim/Andaman and Nicobar Islands/Andaman and Nicobar Islands.csv'):
    dataframe = []
    for folder in os.listdir('data/raw/Andaman and Nicobar Islands/North And Middle Andaman/2015'):
        folder_path = os.path.join('data/raw/Andaman and Nicobar Islands/North And Middle Andaman/2015',folder)
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path,file)
            print(file_path)
            if len(dataframe)==0:
                dataframe = pd.read_csv(file_path)
                columns = dataframe.columns[:-3]
                dataframe = dataframe[columns]
            else:
                df = pd.read_csv(file_path)
                columns = df.columns[:-3]
                df = df[columns]
                dataframe = pd.concat([dataframe, df], ignore_index=True)
    dataframe.to_csv('data/interim/Andaman and Nicobar Islands/Andaman and Nicobar Islands.csv', index=False, encoding='utf-8')

for state in os.listdir(raw_path):
    if state == 'Andaman and Nicobar Islands':
        continue
    if len(state.split('.')) == 1:
        state_path = os.path.join(raw_path,state)
        for year in years:
            temp_path = 'data/interim'
            temp_path = os.path.join(temp_path, state)
            temp_path = os.path.join(temp_path, year+'.csv')
            if os.path.isfile(temp_path):
                continue
            dataframe = []
            for dis in os.listdir(state_path):
                dis_path = os.path.join(state_path,dis)
                dis_path = os.path.join(dis_path,year)
                if os.path.isdir(dis_path):
                    for month in os.listdir(dis_path):
                        month_path = os.path.join(dis_path, month)
                        for day in os.listdir(month_path):
                            day_path = os.path.join(month_path,day)
                            if len(dataframe)==0:
                                dataframe = pd.read_csv(day_path)
                                columns = dataframe.columns[:-3]
                                dataframe = dataframe[columns]
                            else:
                                df = pd.read_csv(day_path)
                                columns = df.columns[:-3]
                                df = df[columns]
                                dataframe = pd.concat([dataframe, df], ignore_index=True)
                            print(day_path)
            if not os.path.isdir(os.path.join('data/interim',state)):
                os.mkdir(os.path.join('data/interim',state))
            dataframe.to_csv(temp_path, index=False, encoding='utf-8')

# consolidate all years into one csv file for all states
interim_path = 'data/interim'

for state in os.listdir(interim_path):
    if state=='.gitkeep':
        continue
    state_path = os.path.join(interim_path,state)
    save_path = os.path.join(state_path,str(state)+'.csv')
    if not os.path.isfile(save_path):
        dataframe = []
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path,year)
            print(year_path)
            df = pd.read_csv(year_path)
            if len(dataframe)==0:
                dataframe = df
            else:
                dataframe = pd.concat([dataframe, df], ignore_index=True)
        dataframe.to_csv(save_path, index=False, encoding='utf-8')

# store the final output
dataframe = []

for state in os.listdir(interim_path):
    if state=='.gitkeep':
        continue
    state_path = os.path.join(interim_path,state)
    file_path = os.path.join(state_path,str(state)+'.csv')
    print(file_path)
    df = pd.read_csv(file_path)
    if len(dataframe)==0:
        dataframe = df
    else:
        dataframe = pd.concat([dataframe, df], ignore_index=True)

# save the final consolidated file in an interim folder
dataframe.to_csv('data/interim/merge_data.csv', index=False, encoding='utf-8')