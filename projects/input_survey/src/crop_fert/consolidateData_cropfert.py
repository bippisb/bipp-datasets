import pandas as pd
from pathlib import Path
import glob
from urllib.parse import quote, unquote
import json

# Define base and state directories
base_dir = Path(__file__).parent.parent.parent
state_dir = base_dir/"data"/"processed"/"2016"

lgd_map_file = open(base_dir/"src"/"LGD_map.json")
lgd_data = json.load(lgd_map_file)

crop_map_file = open(base_dir/"src"/"cropFert_map.json")
crop_data = json.load(crop_map_file)

#Functions to quote and unquote name 
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


def districtLGDMapping(districtname,statename):
    # print(districtname)
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


# List to store dictionaries containing data
list = []

# Function to consolidate data from TABLE4A directories
def consolidateTable4A():
    # Loop through state directories
    for state_file in glob.glob(str(state_dir) + '/*'):
        stateName = unquote_name(state_file.replace(str(state_dir) + '/', ""))
        
        # Loop through district directories within each state
        for district_file in glob.glob(str(state_file) + '/*'):
            districtName = unquote_name(district_file.replace(str(state_file) + '/', ""))
            
            # Construct path to TABLE4A directory for each district
            table_dir = district_file + '/TABLE4A-USAGE%20OF%20DIFFERENT%20FERTILIZERS%20FOR%20DIFFERENT%20CROPS'
            tableName = unquote_name('/TABLE4A-USAGE%20OF%20DIFFERENT%20FERTILIZERS%20FOR%20DIFFERENT%20CROPS')

            # Loop through crop directories within each TABLE4A directory
            for crop_file in glob.glob(str(table_dir) + '/*'):
                cropName = unquote_name(crop_file.replace(str(table_dir) + '/', ""))
                # Loop through fertilizer files within each crop directory
                for fert_file in glob.glob(str(crop_file) + '/*'):
                    fertName = unquote_name((fert_file.replace(str(crop_file) + '/', "")).replace(".csv", ""))
                    data = pd.read_csv(fert_file)

                    # Skip if data is empty
                    if len(data) == 0:
                        continue
                    lgdstateName,lgdstateCode,lgddistrictName,lgddistrictCode = districtLGDMapping(districtName,stateName)
                    cropType,cropCode = cropMapping(cropName)
                    # Iterate through data rows and create dictionaries
                    for i in range(len(data)):
                        inputsurvey_corp_fert_table = {}
                        inputsurvey_corp_fert_table["state_name"] = lgdstateName
                        inputsurvey_corp_fert_table["state_code"] = lgdstateCode
                        inputsurvey_corp_fert_table["district_name"] = lgddistrictName
                        inputsurvey_corp_fert_table["district_code"] = lgddistrictCode
                        inputsurvey_corp_fert_table["crop_name"] = cropName.title()
                        inputsurvey_corp_fert_table["fertlizer"] = fertName.title()
                        inputsurvey_corp_fert_table["crop_type"] = cropType
                        inputsurvey_corp_fert_table["crop_code"] = cropCode
                        inputsurvey_corp_fert_table["farm_size_category"] = data.loc[i, "Farm_size_category"].title()
                        inputsurvey_corp_fert_table["farm_size_class"] = data.loc[i, "Farm_size_class"].title()
                        inputsurvey_corp_fert_table["water_source"] = data.loc[i, "Water_source"]
                        inputsurvey_corp_fert_table["tq_tot_fr"] = data.loc[i, "TOTAL FERT APPLIED_TOTAL"]
                        inputsurvey_corp_fert_table["tot_c1_ar_tr_fr"] = data.loc[i, "AREA TREATED WITH ONE OR MORE FERT_TOTAL"]
                        inputsurvey_corp_fert_table["hol_c1_n_fr"] = data.loc[i, "NO. OF HOLDINGS_NO. TREATED WITH ONE OR MORE CHEMICAL FERTILIZER"]

                        # Append dictionary to the list
                        list.append(inputsurvey_corp_fert_table)
    # Dump the list into a JSON file
    # with open(base_dir/"consolidatedJSON.json", "w") as final:
    #     json.dump(list, final)
    df = pd.DataFrame(list)
    df.to_csv(base_dir/"consolidatedDataCropFert.csv")

# Main function that calls the data consolidation function
def main():
    consolidateTable4A()

# Entry point of the script
if __name__ == "__main__":
    main()