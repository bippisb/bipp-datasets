# import libraries
import pandas as pd
import numpy as np
import os

# initialize an empty list where we store all the data in the form of 2D matrix
data=[]
ladakh_data=[]

# function to read csv data at an input path and store its data into list
def store_data(path):
    df=pd.read_csv(path)
    # add the month column to the dataframe from filename
    date = path.split("\\")[-1].split(".")[0]
    d =[d for d in date]
    d[2] = "-"+d[2]
    date = "".join(d)
    df['month']=date
    # converting pandas dataframe into numpy matrix
    df=np.array(df)
    status='not added'
    # iterating over all the rows and append them into list
    for row in df:
        if row[0]=='Charki Dadri':
            if status=='not added':
                status='added'
            else:
                continue
        temp=row[0]
        row[0]=row[2]
        row[2]=temp
        if row[2]=="Paraganas North" or row[2]=="Paraganas South":
            row[2]='24 '+row[2]

        if row[1]=="Haryana" and row[2]=="Mewat":
            row[2] = "Nuh"

        if row[1]=="Punjab" and row[2]=="Nawanshahr":
            row[2]="Shahid Bhagat Singh Nagar"

        if row[1]=="Uttar Pradesh" and row[2]=="Faizabad":
            for i in range(len(data)-1,0,-1):
                if data[i][2]=="Ayodhya":
                    idx = i
                    break
            for i in range(4,10):
                row[i] = str(row[i])
                data[idx][i] = str(data[idx][i])
                if len(row[i])!=1 and len(data[idx][i])!=1:
                    data[idx][i]=str(float(row[i])+float(data[idx][i]))
                elif len(data[idx][i])==1:
                    data[idx][i]=row[i]
            continue

        if row[1]=="Jammu And Kashmir" and (row[2]=="Leh Ladakh" or row[2]=="Kargil"):
            row[1]="Ladakh"
            ladakh_data.append(row)
            continue

        if row[1]=="Jammu And Kashmir" and row[2]=="Total":
            row1 = []
            for i in range(len(row)):
                if i==1:
                    row1.append("Ladakh")
                elif i>=4 and i<len(row):
                    row1.append(" ")
                else:
                    row1.append(row[i])

            ladakh_data.append(row1)
        data.append(row)

# recursively iterate over all the csv files stored in a row folder
def iterate(path):
    if os.path.isfile(path):
        if path.split('.')[-1]=='csv':
            store_data(path)
        return
    for file in os.listdir(path):
        iterate(os.path.join(path,file))

path='../data/raw'
iterate(path)

for i,row in enumerate(ladakh_data):
    data.append(row)

# columns name
columns = [
        
        "fin_year",
        "state_name",
        "dist_name",
        "fin_month",
        "fin_target_drip",
        "fin_target_sprinkler",
        "fin_total_target",
        "fin_achivement_drip",
        "fin_achivement_sprinkler",
        "fin_total_achievement",
        "percentage_achievement",
        "month",
    ]

# converting the numpy data into pandas dataframe
df=pd.DataFrame(data,columns=columns)
# df.drop([['fin_year','fin_month']],axis=1,inplace=True)
# store pandas dataframe at mentioned path
df.to_csv(os.path.join('../data/interim','output.csv'),index=False)
