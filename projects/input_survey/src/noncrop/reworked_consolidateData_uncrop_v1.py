import pandas as pd
from pathlib import Path
import glob
from urllib.parse import quote, unquote
import json
from functools import reduce
import re

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

              
def consolidateTable2AB_8_9A():
    list = []
    for state_file in glob.glob(str(state_dir)+'/*'):

        stateName = unquote_name(state_file.replace(str(state_dir)+'/',""))

        for district_file in glob.glob(str(state_file)+'/*'):
            
            districtName = unquote_name(district_file.replace(str(state_file)+'/',""))

            tables = ["TABLE2A-IRRIGATED%20AREA%20CROPPED%20ONCE%20%26%20MORE%20THAN%20ONCE","TABLE2B-UN-IRRIGATED%20AREA%20CROPPED%20ONCE%20%26%20MORE%20THAN%20ONCE","TABLE8-CREDIT%20AVAILED%20FOR%20AGRIL.%20PURPOSE","TABLE9A-USAGE%20OF%20CERTIFIED%20SEEDS%20%28BLUE%20TAG%29"]

            for i in range(6):
                inputsurvey_corp_fert_table = {}
                
                inputsurvey_corp_fert_table["year"] = "2016"
                inputsurvey_corp_fert_table["state_name"] =stateName
                inputsurvey_corp_fert_table["state_code"] = "Def"
                inputsurvey_corp_fert_table["district_name"] =districtName
                inputsurvey_corp_fert_table["district_code"] = "Def"
                
                
                for table_file in tables:
                    
                    table_dir = district_file + f'/{table_file}.csv'
                    if Path(table_dir).is_file() == False:
                        continue

                    data = pd.read_csv(table_dir)

                    # If the table is empty
                    if len(data) == 0:
                        continue

                    if "TABLE2A" in table_file:
                        # Here SIZE GROUP(HA) contains the farmSize and farmCategory. Ex: MARGINAL (BELOW 1.0)
                        sizeGroup_value = data.loc[i,"SIZE GROUP(HA)"]
                        matches = re.search(r'(\S+)\s*\((.*?)\)', sizeGroup_value)
                        
                        if i==5: # Ex: ALL GROUPS. our above re search doesn't give a group for this case.
                            inputsurvey_corp_fert_table["farm_size_category"] = "ALL"
                            inputsurvey_corp_fert_table["farm_size_class"] = "ALL"
                        
                        else: # Ex:LARGE (10 AND ABOVE). 
                            inputsurvey_corp_fert_table["farm_size_category"] = matches.group(1).strip()
                            inputsurvey_corp_fert_table["farm_size_class"] = matches.group(2).strip()

                        inputsurvey_corp_fert_table["net_ir_a"] =str(data.loc[i,"Net Irrigated Area"])
                        inputsurvey_corp_fert_table["grs_ir_a"] = str(data.loc[i,"Gross Irrigated Area"])

                    if "TABLE2B" in table_file:
                        inputsurvey_corp_fert_table["unir_netunir_a"] =str(data.loc[i,"UnIrriArea_NetUnIrriArea"])
                        inputsurvey_corp_fert_table["unir_grsunir_a"] = str(data.loc[i,"UnIrriArea_GrossUnIrriArea"])
                    
                    if "TABLE8" in table_file:
                        inputsurvey_corp_fert_table["op_hol_tn"] =str(data.loc[i,"TotalNumOfOperationalHolding"])
                        inputsurvey_corp_fert_table["es_oh_ins_cr"] = str(data.loc[i,"EstNumOperationalHoldings_TookInstitutionalCredit"])
                        inputsurvey_corp_fert_table["oh_cr_pacs"] =str(data.loc[i,"TookInstiCredit_FromPACS"])
                        inputsurvey_corp_fert_table["oh_cr_pldb"] =str(data.loc[i,"TookInstiCredit_FromPLDB/SLDB"])
                        inputsurvey_corp_fert_table["oh_cr_cbb"] =str(data.loc[i,"TookInstiCredit_FromCBB"])
                        inputsurvey_corp_fert_table["oh_cr_rrbb"] =str(data.loc[i,"TookInstiCredit_FromRRBB"])
                        inputsurvey_corp_fert_table["amt_ins_cr_sl"] =str(data.loc[i,"AmountCreditTaken(RS)_SL"])
                        inputsurvey_corp_fert_table["amt_ins_cr_ml"] =str(data.loc[i,"AmountCreditTaken(RS)_ML"])
                        inputsurvey_corp_fert_table["amt_ins_cr_ll"] =str(data.loc[i,"AmountCreditTaken(RS)_LL"])
                        
                        # Didn't find the appropriate column for below value point  
                        # inputsurvey_corp_fert_table["amt_ins_cr_tot"] =str(data.loc[i,""]

                    if "9" in table_file:
                        inputsurvey_corp_fert_table["hol_n_cs"] =str(data.loc[i,"NumOfHoldUsingCertSeeds"])
                        inputsurvey_corp_fert_table["hol_n_hs"] = str(data.loc[i,"NumOfHoldUsingHybridSeeds"])
                        
                        # Didn't find the appropriate column for below value point  
                        # inputsurvey_corp_fert_table["hol_n_nv"] = str(data.loc[i,""]

                   
                list.append(inputsurvey_corp_fert_table)
    
    # with open(base_dir/"consolidated_CropCompostiesTable2AB_8_9AJSON.json", "w") as final:
    #     json.dump(list, final)

    df = pd.DataFrame(list)
    df.to_csv(base_dir/"consolidatedDataNonCrop.csv")   
            
    

def main():
    consolidateTable2AB_8_9A()


if __name__ == "__main__":
    main()