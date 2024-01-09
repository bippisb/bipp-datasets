import pandas as pd
from pathlib import Path
import glob
from urllib.parse import quote, unquote
import json

base_dir = Path(__file__).parent.parent
state_dir = base_dir/"processed"/"2016"


def quote_name(text: str):
    return quote(text.replace("/", "__or__"))


def unquote_name(text: str):
    return unquote(text.replace("__or__", "/"))

stateCodeDict = {
    "A N ISLANDS": "23a",
    "ANDHRA PRADESH":  "1a",
    "ARUNACHAL PRADESH": "24a",
    "ASSAM": "2a",
    "BIHAR": "3a",
    "CHANDIGARH": "31a",
    "CHATTISGARH": "34a",
    "D N HAVELI": "25a",        
    "DAMAN DIU" : "32a",      
    "DELHI": "26a",
    "GOA": "27a",       
    "GUJARAT": "4a",        
    "HARYANA":"5a",        
    "HIMACHAL PRADESH": "6a",        
    "JAMMU KASHMIR": "7a",    
    "JHARKHAND": "35a",        
    "KARNATAKA": "8a",        
    "KERALA" : "9a",        
    "LAKSHADWEEP" : "30a",     
    "MADHYA PRADESH" : "10a",       
    "MAHARASHTRA":"11a",        
    "MANIPUR":"12a",        
    "MEGHALAYA":"13a",        
    "MIZORAM":"28a",     
    "NAGALAND":"14a",     
    "ODISHA":"15a",  
    "PUDUCHERRY":"29a",     
    "PUNJAB":"16a",
    "RAJASTHAN":"17a",  
    "SIKKIM":"18a",
    "TAMIL NADU":"19a",    
    "TELENGANA":"37a", 
    "TRIPURA":"20a",
    "UTTAR PRADESH":"21a",        
    "UTTARAKHAND":"36a",        
    "WEST BENGAL":"22a",  
}

districtCodeDict = {
    "TELENGANA": {
        "ADILABAD" : "19",
        "KARIMNAGAR" : "20",
        "KHAMMAM" : "22",
        "MAHABUBNAGAR" : "14",
        "MEDAK" : "17",
        "NALGONDA" : "23",
        "NIZAMABAD" : "18",
        "RANGAREDDY" : "15",
        "WARANGAL" : "21",
    }
}

list = []

def consolidateTable4A():

    for state_file in glob.glob(str(state_dir)+'/*'):

        stateName = unquote_name(state_file.replace(str(state_dir)+'/',""))

        for district_file in glob.glob(str(state_file)+'/*'):
            districtName = unquote_name(district_file.replace(str(state_file)+'/',""))
            
            table_dir = district_file + '/TABLE4A-USAGE%20OF%20DIFFERENT%20FERTILIZERS%20FOR%20DIFFERENT%20CROPS'
            tableName = unquote_name('/TABLE4A-USAGE%20OF%20DIFFERENT%20FERTILIZERS%20FOR%20DIFFERENT%20CROPS')

            for crop_file in glob.glob(str(table_dir)+'/*'):
                cropName = unquote_name(crop_file.replace(str(table_dir)+'/',""))      
                
                for fert_file in glob.glob(str(crop_file)+'/*'):
                    fertName = unquote_name( (fert_file.replace(str(crop_file)+'/',"")).replace(".csv","") )           
                    data = pd.read_csv(fert_file)

                    if len(data) == 0:
                        continue

                    for i in range(len(data)):
                        inputsurvey_corp_fert_table = {}
                        inputsurvey_corp_fert_table["state_name"] =stateName
                        inputsurvey_corp_fert_table["state_code"] = "Def"
                        inputsurvey_corp_fert_table["district_name"] =districtName
                        inputsurvey_corp_fert_table["district_code"] = "Def"
                        inputsurvey_corp_fert_table["crop_name"] = cropName
                        inputsurvey_corp_fert_table["fertlizer"] = fertName
                        inputsurvey_corp_fert_table["crop_type"] = "Def"
                        inputsurvey_corp_fert_table["crop_code"] = "Def"
                        inputsurvey_corp_fert_table["farm_size_category"] = data.loc[i, "Farm_size_category"]
                        inputsurvey_corp_fert_table["farm_size_class"] = data.loc[i, "Farm_size_class"]
                        inputsurvey_corp_fert_table["water_source"] = data.loc[i,"Water_source"]
                        inputsurvey_corp_fert_table["tq_tot_fr"] = data.loc[i,"TOTAL FERT APPLIED_TOTAL"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_fr"] = data.loc[i,"AREA TREATED WITH ONE OR MORE FERT_TOTAL"]
                        inputsurvey_corp_fert_table["hol_c1_n_fr"] = data.loc[i,"NO. OF HOLDINGS_NO. TREATED WITH ONE OR MORE CHEMICAL FERTILIZER"]
                    
                        list.append(inputsurvey_corp_fert_table)
    
    # with open(base_dir/"consolidatedJSON.json", "w") as final:
    #     json.dump(list, final)

    df = pd.DataFrame(list)
    df.to_csv(base_dir/"consolidatedDataCropFert.csv")

def main():
    consolidateTable4A()

if __name__ == "__main__":
    main()