import pandas as pd
from pathlib import Path
import glob
from urllib.parse import quote, unquote
import json
from functools import reduce
import re

base_dir = Path(__file__).parent.parent.parent
state_dir = base_dir/"data"/"processed"/"2016"

lgd_map_file = open(base_dir/"data"/"raw"/"LGD_map.json")
lgd_data = json.load(lgd_map_file)

def quote_name(text: str):
    return quote(text.replace("/", "__or__"))


def unquote_name(text: str):
    return unquote(text.replace("__or__", "/"))

districtNames_Dict = {
    'NELLORE':"Sri Potti Sriramulu Nellore", 
    'KADAPA': "Y.S.R.", 
    'ANANTAPUR': "Ananthapuramu", 
    'NARSINGPUR': "Narsimhapur", 
    'SHEOPUR KALAN': "Sheopur", 
    'ASHOK NAGAR': "Ashoknagar", 
    'KAHNDWA': "Khandwa (East Nimar)", 
    'KHARGON':"Khargone (West Nimar)", 
    'AAGAR':"Agar-Malwa",
    'HOSANGABAD':"Narmadapuram", 
    'BADWANI':"Barwani", 
    'KANCHEEPURAM':"Kanchipuram", 
    'TIRUVALLUR':"Thiruvallur", 
    'PUDUKOTTAI':"Pudukkottai", 
    'TIRUCHIRAPALLI':"Tiruchirappalli", 
    'SIVAGANGAI':"Sivaganga", 
    'KANYAKUMARI':"Kanniyakumari", 
    'NORTH DINAJPUR':"Uttar Dinajpur", 
    'PURBA BURDWAN':"Purba Bardhaman", 
    'PASCHIM MEDDINIPUR':"Paschim Medinipur", 
    'PASCHIM BURDWAN':"Paschim Bardhaman", 
    'HOOGLY':"Hooghly", 
    'SOUTH DINAJPUR':"Dakshin Dinajpur", 
    'PURBA MEDDINIPUR':"Purba Medinipur", 
    'NORTH & MIDDLE ANDAMAN':"North And Middle Andaman", 
    'NICOBAR':"Nicobars", 
    'SOUTH ANDAMAN':"South Andamans", 
    'MAHINDERGARH':"Mahendragarh", 
    'HISSAR':"Hisar", 
    'GURGAON':"Gurugram", 
    'MEWAT':"Nuh", 
    'PANCHMAHALS':"Panch mahals", 
    'DANGS':"Dang", 
    'SABARKANTHA':"Sabar Kantha", 
    'BANASKANTHA':"Banas Kantha", 
    'DAHOD':"Def", 
    'CHAMARAJANAGAR':"Chamarajanagara", 
    'CHICHBALLAPURA':"Chikkaballapura", 
    'KALABURGI':"Kalaburagi", 
    'VIJAPURA':"Vijayapura", 
    'RAMANAGAR':"Ramanagara", 
    'BENGALURU (U )':"Bengaluru Urban", 
    'UTTRA KANNADA':"Uttara Kannada", 
    'DAVANAGERE':"Davangere", 
    'CHIKKAMAGALUR':"Chikkamagaluru", 
    'BENGALURU (R )':"Bengaluru Rural", 
    'BULDANA':"Buldhana", 
    'OSMANABAD':"Dharashiv", 
    'SANGALI':"Sangli", 
    'JAJPUR':"Jajapur", 
    'BALASORE':"Baleshwar", 
    'KHURDA':"Khordha", 
    'JAGATSINGHPUR':"Jagatsinghapur", 
    'NAWARANGPUR':"Nabarangpur", 
    'KEONJHAR':"Kendujhar", 
    'JHARASUGUDA':"Jharsuguda", 
    'ANGUL':"Anugul", 
    'RANGAREDDY':"Ranga Reddy", 
    'BARABANKI':"Bara Banki", 
    'KUSHI NAGAR':"Kushinagar", 
    'SIDDHARTH NAGAR':"Siddharthnagar", 
    'SANT RAVIDAS NAGAR':"Bhadohi", 
    'BADAAYU':"Budaun", 
    'RAEBARELI':"Rae Bareli", 
    'ALLAHABAD':"Prayagraj", 
    'RAMABAI NAGAR/KANPUR DEHA':"Kanpur Dehat", 
    'GAUTAM BUDDH NAGAR':"Gautam Buddha Nagar", 
    'CHOTRAPATI SAHOOJI MAHARA':"Amethi", 
    'MAHARAJGANJ':"Mahrajganj", 
    'BULANDSHAHAR':"Bulandshahr", 
    'SONEBHADRA':"Sonbhadra", 
    'FAIZABAD':"Firozabad", 
    'BEHRAICH':"Bahraich", 
    'MAHA MAYA NAGAR':"Hathras", 
    'KASHIRAM NAGAR':"Kasganj", 
    'JYOTIBA PHULE NAGAR':"Amroha", 
    'LAHUL/SPITI':"Lahul And Spiti", 
    'KAMRUP (RURAL)':"Kamrup", 
    'MORIGAON':"Marigaon", 
    'KARBI-ANGLONG':"Karbi Anglong", 
    'DIMA HASAO (N. C. HILLS)':"Dima Hasao", 
    'ODALGURI':"Udalguri", 
    'KAMRUP (METRO)':"Kamrup Metro", 
    'RI-BHOI':"Ri Bhoi", 
    'BALODABAZAR':"Balodabazar-Bhatapara", 
    'SARGUJA':"Surguja", 
    'JANJGIR CHAMPA':"Janjgir-Champa", 
    'KANKER':"Uttar Bastar Kanker", 
    'KABIRDHAM(KAWARDHA)':"Kabeerdham", 
    'BANDIPURA':"Bandipora", 
    'UDAMPUR':"Udhampur", 
    'LEH':"Leh Ladakh", 
    'DADRA & NAGAR HAVELI':"Dadra And Nagar Haveli", 
    'RUDRAPRAYAG':"Rudra Prayag", 
    'UTTARKASHI':"Uttar Kashi", 
    'UDHAMSINGHNAGAR':"Udam Singh Nagar", 
    'PUDUCHERRY':"Pondicherry", 
    'NAWADAH':"Nawada", 
    'KAIMUR':"Kaimur (Bhabua)", 
    'EAST CHAMPARAN':"Purbi Champaran", 
    'PURNEA':"Purnia", 
    'WEST CHAMPARAN':"Pashchim Champaran", 
    'EAST SINGHBHUM':"Def"
}

def districtLGDMapping(districtname,statename):
    if (districtname.title()) in lgd_data:
            lgdstateName = lgd_data[districtname.title()]["StateName"]
            lgdstateCode = lgd_data[districtname.title()]["StateLGDCode"]
            lgddistrictName = lgd_data[districtname.title()]["DistrictName"]
            lgddistrictCode = lgd_data[districtname.title()]["DistictLGDcCode"]
    else:   
        if districtname in districtNames_Dict:
            if (districtNames_Dict[districtname].title()) in lgd_data:
                lgdstateName = lgd_data[districtNames_Dict[districtname].title()]["StateName"]
                lgdstateCode = lgd_data[districtNames_Dict[districtname].title()]["StateLGDCode"]
                lgddistrictName = lgd_data[districtNames_Dict[districtname].title()]["DistrictName"]
                lgddistrictCode = lgd_data[districtNames_Dict[districtname].title()]["DistictLGDcCode"]
            else:
                lgdstateName = statename.title()
                lgdstateCode = "Def"
                lgddistrictName = districtname.title()
                lgddistrictCode = "Def"
        else:
            print("Problem:",districtname)
            lgdstateName = statename.title()
            lgdstateCode = "Def"
            lgddistrictName = districtname.title()
            lgddistrictCode = "Def"

    return lgdstateName,lgdstateCode,lgddistrictName,lgddistrictCode

              
def consolidateTable2AB_8_9A():
    list = []
    for state_file in glob.glob(str(state_dir)+'/*'):

        stateName = unquote_name(state_file.replace(str(state_dir)+'/',""))

        for district_file in glob.glob(str(state_file)+'/*'):
            
            districtName = unquote_name(district_file.replace(str(state_file)+'/',""))

            tables = ["TABLE2A-IRRIGATED%20AREA%20CROPPED%20ONCE%20%26%20MORE%20THAN%20ONCE","TABLE2B-UN-IRRIGATED%20AREA%20CROPPED%20ONCE%20%26%20MORE%20THAN%20ONCE","TABLE8-CREDIT%20AVAILED%20FOR%20AGRIL.%20PURPOSE","TABLE9A-USAGE%20OF%20CERTIFIED%20SEEDS%20%28BLUE%20TAG%29"]
            
            lgdstateName,lgdstateCode,lgddistrictName,lgddistrictCode = districtLGDMapping(districtName,stateName)

            for i in range(6):
                inputsurvey_corp_fert_table = {}
                
                inputsurvey_corp_fert_table["year"] = "2016"
                inputsurvey_corp_fert_table["state_name"] =lgdstateName
                inputsurvey_corp_fert_table["state_code"] = lgdstateCode
                inputsurvey_corp_fert_table["district_name"] =lgddistrictName
                inputsurvey_corp_fert_table["district_code"] = lgddistrictCode
                
                
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

                    if "TABLE9" in table_file:
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