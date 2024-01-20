import pandas as pd
from pathlib import Path
import glob
from urllib.parse import quote, unquote
import json
from functools import reduce

# Set base directories and paths
base_dir = Path(__file__).parent.parent.parent
state_dir = base_dir/"data"/"processed"/"2016"

lgd_map_file = open(base_dir/"src"/"LGD_map.json")
lgd_data = json.load(lgd_map_file)

crop_map_file = open(base_dir/"src"/"cropFert_map.json")
crop_data = json.load(crop_map_file)

# Functions to quote special characters in a name
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
    'DAHOD':"Dahod", 
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
    'EAST SINGHBHUM':"East Singhbum",
    "MINICOY":"Minicory",
    "ANDROTT":"Andrott",
    "AMINI":"Amini",
    "KAVARATTI":"Kavaratti",
    "PAPUMPARE": "Papum Pare",
    "DANTEWADA":"Dakshin Bastar Dantewada",
    "ROPAR":"Rupnagar",
    "SHAHED BHAGAT SINGH NAGAR":"Shahid Bhagat Singh Nagar",
    "TARN-TARAN":"Tarn Taran",
    "MOHALI":"S.A.S Nagar",
    "MUKTSAR":"Sri Muktsar Sahib"
}

cropNames_Dict = {
    "Total Oil Seeds":"Def",
    'Moong':"Def",
    "Ladys Finger (Bhindi)": "Lady's finger", 
    "Total Aromatic And Medicinal Plants":"Def", 
    "Gram":"Def", 
    'Total Non-Food Crops':"Def", 
    'Beans (Green)':"Def", 
    'Total Food Crops':"def", 
    'Mulberry Crop':"Mullberry Crop", 
    'Chillies':"Dry Chillies", 
    'Horsegram':"Def", 
    'Total Spices & Condiments':"Def", 
    'Total Fibres':"Def",
    'Tur (Arhar)':"Def",
    'Urad':"Def", 
    'All Vegetables':"Def", 
    'Ragi':"Def", 
    'Total Foodgrains':"Def", 
    'Lemon / Acid Lime':"Lime", 
    'Total Sugar Crops':"Def", 
    'Total Fruits':"Def", 
    'Total Cereals':"Def", 
    'Cashewnuts':"Cashew", 
    # 'Mangoes', 
    # 'Other Non-Food Crops', 
    # 'Total Pulses', 
    # 'Sesamum (Til)', 
    # 'Total Other Non-Food Crops', 
    # 'Casuarina', 'Watermelon', 'Ginger', 'Bajra', 'Beetlvine', 'Bottle Gourd (Lauki)', 
    # 'Green Chillies', 'Other Gourd', 'Total Plantation Crops', 'Fodder & Green Manures', 
    # 'Green Manures', 'Other Plantation Crops', 'Tapioca (Cassava)', 'Mosambi', 'Merigold', 
    # 'Total Drugs & Narcotics', 'Total Floriculture Crops', 'Jowar', 'Castorseed', 'Ajwain', 
    # 'Beans (Pulses)', 'Chiku', 'Peas (Vegetable) (Green)', 'Chrysanthemum', 'Masur', 'Coriander', 
    # 'Plantain', 'Rapeseed & Mustard (Toria/Taramira)', 'Other Condi. & Spices', 
    # 'Betelnuts (Arecanuts)', 'Orange', 'Vench (Guar)', 'Peas (Pulses)', 'Strawberry', 
    # 'Turnip (Shalgam)', 'Amaranths (Chaulai)', 'Table Grapes', 'Palmvriah', 'Spinach', 
    # 'Cardamom (Large)', 'Colocasia/Arum', 'Pepper (Black)', 'Cardamom (Small)', 
    # 'Total Dyes & Tanning Materials', 'Fenugreek', 'Elephant Foot Yam', 'Asgandh', 
    # 'Isabgol', 'Jack Fruit', 'Nigerseed', 'Tuberose', 'Aonla (Amla)', 'Wine Grapes (Black)', 
    # 'Koval (Little Gourd)', 'Ber', 'Other Citrus Fruits', 'Carnation', 'Gladiolus', 
    # 'Tinda', 'Fennel / Anise Seed', 'Lichi', 'Peaches', 'Orchids', 'Vanilla', 'Kinnoo', 
    # 'Gaillardia', 'Remputan', 'Gerbera', 'Allovera', 'Other Dyes & Tanning Materials'
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

    return lgdstateName,lgdstateCode,lgddistrictName,lgddistrictCode

def cropMapping(cropname):
    if(cropname.title()) in crop_data:
        cropcode = crop_data[cropname.title()]["cropCode"]
        croptype = crop_data[cropname.title()]["cropType"]
    else:
        if cropname.title() in cropNames_Dict:
            if (cropNames_Dict[cropname.title()]) in crop_data:
                cropcode = crop_data[cropNames_Dict[cropname.title()]]["cropCode"]
                croptype = crop_data[cropNames_Dict[cropname.title()]]["cropType"]
            else:
                return "Def","Def"
        else:
            return "Def","Def"
            
    return cropcode,croptype

# Function to consolidate data from TABLE4 for different crops
def consolidateTable4():
    list = []

    for state_file in glob.glob(str(state_dir)+'/*'):

        stateName = unquote_name(state_file.replace(str(state_dir)+'/',""))

        for district_file in glob.glob(str(state_file)+'/*'):
            districtName = unquote_name(district_file.replace(str(state_file)+'/',""))

            table_dir = district_file + '/TABLE4-USAGE%20OF%20FERTILIZERS%20FOR%20DIFFERENT%20CROPS'
            tableName = unquote_name('/TABLE4-USAGE%20OF%20FERTILIZERS%20FOR%20DIFFERENT%20CROPS')

            for crop_file in glob.glob(str(table_dir)+'/*'):
                cropName = unquote_name(crop_file.replace(str(table_dir)+'/',"")).replace(".csv", "")
                
                crop_dir = table_dir+f'/{cropName}.csv'
                if Path(crop_dir).is_file() == False:
                        continue
                
                data = pd.read_csv(crop_file)
             
                # If the table is empty
                if len(data) == 0:
                    continue
                
                lgdstateName,lgdstateCode,lgddistrictName,lgddistrictCode = districtLGDMapping(districtName,stateName)
                cropType,cropCode = cropMapping(cropName)

                for i in range(len(data)):

                        inputsurvey_corp_fert_table = {}
                        
                        inputsurvey_corp_fert_table["state_name"] =lgdstateName
                        inputsurvey_corp_fert_table["state_code"] = lgdstateCode
                        inputsurvey_corp_fert_table["district_name"] =lgddistrictName
                        inputsurvey_corp_fert_table["district_code"] = lgddistrictCode
                        inputsurvey_corp_fert_table["crop_name"] = cropName
                        inputsurvey_corp_fert_table["crop_type"] = cropType
                        inputsurvey_corp_fert_table["crop_code"] = cropCode
                        inputsurvey_corp_fert_table["farm_size_category"] = data.loc[i, "Farm_size_category"]
                        inputsurvey_corp_fert_table["farm_size_class"] = data.loc[i, "Farm_size_class"]
                        inputsurvey_corp_fert_table["water_source"] = data.loc[i,"Water_source"]
                        inputsurvey_corp_fert_table["hol_c_tn"] = data.loc[i,"NO. OF HOLDINGS_TOTAL NO."]
                        inputsurvey_corp_fert_table["hol_c1_n_fr"] = data.loc[i,"NO. OF HOLDINGS_NO. TREATED WITH ONE OR MORE CHEMICAL FERTILIZER"]
                        inputsurvey_corp_fert_table["hyv_c1_ar"] = data.loc[i,"AREA UNDER_HYV"]
                        inputsurvey_corp_fert_table["ot_c1_ar"]= data.loc[i,"AREA UNDER_OTHERS"]
                        inputsurvey_corp_fert_table["tot_c1_ar"]= data.loc[i,"AREA UNDER_TOTAL"]
                        inputsurvey_corp_fert_table["tot_c1_trmr1_chem_a"] = data.loc[i,"AREA TREATED WITH ONE OR MORE FERT_TOTAL"]
                        inputsurvey_corp_fert_table["tq_ntr_totn"] = data.loc[i,"TOTAL_N"]
                        inputsurvey_corp_fert_table["tq_ntr_totp"] = data.loc[i,"TOTAL_P"]
                        inputsurvey_corp_fert_table["tq_ntr_totk"] = data.loc[i,"TOTAL_K"]
     
                        list.append(inputsurvey_corp_fert_table)
    
    # with open(base_dir/"consolidated_CropCompostiesTable4JSON.json", "w") as final:
    #     json.dump(list, final)

    df = pd.DataFrame(list)
    df.to_csv(base_dir/"consolidatedDataCropFertCompositesTable4.csv")   

# Function to consolidate data from TABLE5 for different crops            
def consolidateTable5():
    list = []
    for state_file in glob.glob(str(state_dir)+'/*'):

        stateName = unquote_name(state_file.replace(str(state_dir)+'/',""))

        for district_file in glob.glob(str(state_file)+'/*'):
            districtName = unquote_name(district_file.replace(str(state_file)+'/',""))
            
                        
            tables = ["TABLE5E-USAGE%20OF%20FYM__or__COMPOST%20FOR%20DIFFERENT%20CROPS","TABLE5F-USAGE%20OF%20OIL%20CAKES%20FOR%20DIFFERENT%20CROPS","TABLE5G-USAGE%20OF%20OTHER%20ORGANIC%20MANURES%20FOR%20DIFFERENT%20CROPS","TABLE5H-USAGE%20OF%20PESTICIDES%20FOR%20DIFFERENT%20CROPS","TABLE5I-USAGE%20OF%20RHIZOBIUM%20FOR%20DIFFERENT%20CROPS","TABLE5K-USAGE%20OF%20BLUE%20GREEN%20ALGAE%20FOR%20DIFFERENT%20CROPS","TABLE5L-USAGE%20OF%20PSB%20FOR%20DIFFERENT%20CROPS","TABLE5LA-USAGE%20OF%20GREEN%20MANURE%20FOR%20DIFFERENT%20CROPS"]
            
            lgdstateName,lgdstateCode,lgddistrictName,lgddistrictCode = districtLGDMapping(districtName,stateName)

            for i in range(18):
                inputsurvey_corp_fert_table = {}
    
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
                    # Assign values based on the type of TABLE5
                    if "5E" in table_file:
                        inputsurvey_corp_fert_table["farm_size_category"] = data.loc[i, "Farm_size_category"]
                        inputsurvey_corp_fert_table["farm_size_class"] = data.loc[i, "Farm_size_class"]
                        inputsurvey_corp_fert_table["water_source"] = data.loc[i,"Water_source"]
                        inputsurvey_corp_fert_table["hol_c_mnr_n"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c_ar_tr_mnr"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                        inputsurvey_corp_fert_table["tq_mnr_tot"] =data.loc[i,"TOTAL MANURE APPLIED_TOTAL"]

                    if "5F" in table_file:
                        inputsurvey_corp_fert_table["hol_c_mnr_n_oil"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c_ar_tr_mnr_oil"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                        inputsurvey_corp_fert_table["tq_mnr_tot_oil"] =data.loc[i,"TOTAL MANURE APPLIED_TOTAL"]
                    
                    if "5G" in table_file:
                        inputsurvey_corp_fert_table["hol_c_mnr_n_org"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c_ar_tr_mnr_org"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                        inputsurvey_corp_fert_table["tq_mnr_tot_org"] =data.loc[i,"TOTAL MANURE APPLIED_TOTAL"]
                    
                    if "5H" in table_file:
                        inputsurvey_corp_fert_table["hol_c1_n_pes"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH PESTICIDES"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_pes"] = data.loc[i,"AREA TREATED PESTICIDES_TOTAL"]
                    
                    if "5I" in table_file:
                        inputsurvey_corp_fert_table["hol_c1_mnr_n_rzm"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_mnr_rzm"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                    
                    if "5J" in table_file:
                        inputsurvey_corp_fert_table["hol_c1_mnr_n_azr"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_mnr_azr"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                    
                    if "5K" in table_file:
                        inputsurvey_corp_fert_table["hol_c1_mnr_n_bga"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_mnr_bga"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                    
                    if "5L" in table_file:
                        inputsurvey_corp_fert_table["hol_c1_mnr_n_psb"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_mnr_psb"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                    
                    if "5LA" in table_file:
                        inputsurvey_corp_fert_table["hol_c1_mnr_n_gmnr"] =data.loc[i,"NO. OF HOLDINGS GROWING CROPS_NO. TREATED WITH MANURE"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_mnr_gmnr"] = data.loc[i,"AREA TREATED MANURE_TOTAL"]
                
                list.append(inputsurvey_corp_fert_table)
    
    # with open(base_dir/"consolidated_CropCompostiesTable5JSON.json", "w") as final:
    #     json.dump(list, final)

    df = pd.DataFrame(list)
    df.to_csv(base_dir/"consolidatedDataCropFertCompositesTable5.csv")    
            
    

def main():
    consolidateTable4()
    consolidateTable5()

if __name__ == "__main__":
    main()