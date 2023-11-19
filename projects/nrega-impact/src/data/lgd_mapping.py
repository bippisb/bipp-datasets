import pathlib

import numpy as np
import pandas as pd
from fuzzywuzzy import process

# Note: The Points of difference between lgd_iterator.py and lgd_mapping.py are identified using the identifier #**. Search replace this identifier in this file to get the iterator version of the script

# Importing LGD mapping index

lgd = pd.read_csv("./data/external/block_lgd_index.csv")

lgd.drop("St_Cs2011_code", axis=1, inplace=True)

# renaming the concerned columns
lgd = lgd.rename(
    columns={
        "State Name(In English)": "state",
        "District Name(In English)": "district",
        "Block Name (In English) ": "block_name",
    }
)


# concatenating states, districts and blocks
lgd["state_dist"] = lgd["state"] + lgd["district"]
lgd["state_dist_block"] = lgd["state"] + lgd["district"] + lgd["block_name"]

# defining a lsit to store not lgd mapped and LGD mapped state details

# **not_lgd_list=[]
# **lgd_list=[]

# loading in the state files


def block_lgd_mapper(path_name):
    print(
        "******************************************************LGD Mapping Initiating******************************************"
    )

    files = list(pathlib.Path(path_name).glob("./data/processed/*.csv"))

    pathlib.Path(str(path_name + "/data/processed/block_lgd_mapped")).mkdir(
        parents=False, exist_ok=True
    )

    for file in files:

        # Filtering each state and its entries
        if file.stem == "ANDAMAN AND NICOBAR":
            # since Andaman is recorded as below in lgd state column, the code breaks.This line serves to override the error. "Don't fret. this has been taken care of!!!"
            lgd_state_filtered = lgd[lgd["state"] == "ANDAMAN AND NICOBAR ISLANDS"]
        else:
            lgd_state_filtered = lgd[lgd["state"] == file.stem]

        print("LGD file filtered for", file.stem)

        # reading in the state csv files
        df = pd.read_csv("./data/processed/" + file.name)
        print(file.stem, "has been loaded")

        # uppercasing the block names in state files
        df["block_name"] = df["block_name"].str.upper()
        df["state"] = df["state"].str.upper()
        df["district"] = df["district"].str.upper()

        #########################   MANUAL MAPPING LEVEL 1   ####################################################
        # function to clean certain district and state level name changes and reformations created by administrative decisions
        def name_cleaner(data):
            if file.stem == "ANDHRA PRADESH":
                data["district"] = data["district"].str.replace("CUDDAPAH", "Y.S.R.")
                data["district"] = data["district"].str.replace(
                    "NELLORE", "SPSR NELLORE"
                )
            elif file.stem == "ARUNACHAL PRADESH":
                data["district"] = data["district"].str.replace(
                    "UPPER DIBANG VALLEY", "DIBANG VALLEY"
                )
            elif file.stem == "HARYANA":
                data["district"] = data["district"].str.replace("MEWAT", "NUH")
            elif file.stem == "KARNATAKA":
                data["district"] = data["district"].str.replace(
                    "BENGALURU", "BENGALURU URBAN"
                )
            elif file.stem == "MADHYA PRADESH":
                data["district"] = data["district"].str.replace("KHANDWA", "EAST NIMAR")
            elif file.stem == "PUNJAB":
                data["district"] = data["district"].str.replace(
                    "MUKATSAR", "SRI MUKTSAR SAHIB"
                )
                data["district"] = data["district"].str.replace(
                    "NAWANSHAHR", "SHAHID BHAGAT SINGH NAGAR"
                )
                data["district"] = data["district"].str.replace("ROPAR", "RUPNAGAR")
                data["district"] = data["district"].str.replace(
                    "SAS NAGAR MOHALI", "S.A.S Nagar"
                )
            elif file.stem == "TAMIL NADU":
                data["district"] = data["district"].str.replace(
                    "THOOTHUKKUDI", "TUTICORIN"
                )
            elif file.stem == "TELANGANA":
                data["district"] = data["district"].str.replace(
                    "MEDCHAL", "MEDCHAL MALKAJGIRI"
                )
                data["district"] = data["district"].str.replace(
                    "SIRSILLA", "RAJANNA SIRCILLA"
                )
                data["district"] = data["district"].str.replace(
                    "YADADRI", "YADADRI BHUVANAGIRI"
                )
                data["district"] = data["district"].str.replace(
                    "JAYASHANKER", "JAYASHANKAR BHUPALAPALLY"
                )
            elif file.stem == "UTTAR PRADESH":
                data["district"] = data["district"].str.replace(
                    "SANT RAVIDAS NAGAR", "BHADOHI"
                )
            elif file.stem == "WEST BENGAL":
                data["district"] = data["district"].str.replace(
                    "PURBA MEDINIPUR", "MEDINIPUR EAST"
                )
                data["district"] = data["district"].str.replace(
                    "DARJEELING GORKHA HILL COUNCIL DGHC", "DARJEELING"
                )

            return data

        df = name_cleaner(df)

        ########################   #MANUAL MAPPING LEVEL 2 #########################################################
        # function to correct specific district names caused by spelling errors and input errors
        # function to clean certain district and state level name changes and reformations created by administrative decisions
        def dist_bifur(data):
            if file.stem == "ARUNACHAL PRADESH":
                conditions = [
                    # For districts in Arunachal Pradesh
                    # west Siang - Shi Yomi
                    data["block_name"].isin(["MONIGONG", "MECHUKHA"]),
                    # west siang - Lower Siang
                    data["block_name"].isin(["GENSI", "LIKABALI"]),
                    # East Siang - Lower Siang
                    data["block_name"].isin(["RAMLE-BANGGO"]),
                    # Wst Siang  to Lower Siang - Leparada
                    data["block_name"].isin(["TIRBIN", "BASAR", "DARING"]),
                    # East Kameng - Pakke kessang
                    data["block_name"].isin(["PAKKE-KESSANG", "SEIJOSA"]),
                ]

                options = [
                    "SHI YOMI",
                    "LOWER SIANG",
                    "LOWER SIANG",
                    "LEPARADA",
                    "PAKKE KESSANG",
                ]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            elif file.stem == "TAMIL NADU":
                conditions = [
                    # For districts in Tamil Nadu
                    # Trirunelveli - Tenkasi
                    data["block_name"].isin(
                        [
                            "ALANKULAM",
                            "KADAYANALLUR",
                            "KADAYAM",
                            "KEELAPAVOOR",
                            "KURUVIKULAM",
                            "MELANEELITHANALLUR",
                            "SANKARANKOIL",
                            "SHENCOTTAI",
                            "TENKASI",
                            "VASUDEVANALLUR",
                        ]
                    ),
                    # Vellore-Ranipet
                    data["block_name"].isin(
                        [
                            "ARAKONAM",
                            "ARCOT",
                            "KAVERIPAKKAM",
                            "NEMILI",
                            "SHOLINGHUR",
                            "THIMIRI",
                            "WALAJAH",
                        ]
                    ),
                    # vellore - Tirupathur
                    data["block_name"].isin(
                        [
                            "ALANGAYAM",
                            "JOLARPET",
                            "KANDHILI",
                            "MADHANUR",
                            "NATRAMPALLI",
                            "THIRUPATHUR",
                        ]
                    ),
                    # villupuram - Kallakurichi
                    data["block_name"].isin(
                        [
                            "CHINNASALEM",
                            "RISHIVANDIYAM",
                            "SANKARAPURAM",
                            "THIYAGADURGAM",
                            "TIRUKOILUR",
                            "TIRUNAVALUR",
                            "ULUNDURPET",
                            "KALLAKURICHI",
                            "KALRAYAN HILLS",
                        ]
                    ),
                    # Kanchipuram - Chengalpattu
                    data["block_name"].isin(
                        [
                            "ACHARAPAKKAM",
                            "THIRUPORUR",
                            "THOMAS MALAI",
                            "CHITHAMUR",
                            "KATTANKOLATHUR",
                            "LATHUR",
                            "MADURANTAKAM",
                            "THIRUKALUKUNDRAM",
                        ]
                    ),
                ]

                options = [
                    "TENKASI",
                    "RANIPET",
                    "TIRUPATHUR",
                    "KALLAKURICHI",
                    "CHENGALPATTU",
                ]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            elif file.stem == "TELANGANA":
                # For districts in Telanagana
                # Mahbubnagar - Naraynpet
                conditions = [
                    data["block_name"].isin(
                        [
                            "KOSGI",
                            "MADDUR",
                            "MAGANOOR",
                            "MAKTHAL",
                            "NARAYANPET",
                            "NARVA",
                            "UTKOOR",
                            "DAMARAGIDDA",
                        ]
                    ),
                    # warrangal - Warrangal rural
                    data["block_name"].isin(
                        [
                            "ATMAKUR",
                            "DUGGONDI",
                            "KHANAPUR",
                            "NALLABELLY",
                            "NEKKONDA",
                            "PARKAL",
                            "SANGEM",
                            "WARDHANNAPET",
                        ]
                    ),
                    # JAYASHANKAR BHUPALAPALLY - Mulug
                    data["block_name"].isin(
                        ["MULUG", "WAZEED", "GOVINDARAOPET", "MANGAPET"]
                    ),
                ]
                options = ["NARAYANPET", "WARANGAL RURAL", "MULUG"]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            elif file.stem == "MANIPUR":
                # for districts in Manipur
                conditions = [
                    # Chandel - Tengnoupal
                    data["block_name"].isin(["MACHI", "MOREH", "TENGNOUPAL"]),
                    # ukhrul -Kamjong
                    data["block_name"].isin(
                        ["KAMJONG", "KASOM KHULLEN", "SAHAMPHUNG TD BLOCK"]
                    ),
                    # Churachandpur - Pherzawl
                    data["block_name"].isin(["THANLON", "TIPAIMUKH"]),
                    # senapati - Kangpokpi
                    data["block_name"].isin(
                        [
                            "BUNGTE CHIRU  TD BLOCK",
                            "SAIKUL",
                            "SAITU GAMPHAZOL",
                            "T. VAICHONG TD BLOCK",
                            "CHAMPHAI TD BLOCK",
                            "ISLAND TD BLOCK",
                            "KANGCHUP GELJANG TD BLOCK",
                            "KANGPOKPI",
                            "LHUNGTIN  TD BLOCK",
                        ]
                    ),
                    # Imphal east - Jiribam
                    data["block_name"].isin(["BOROBEKRA CD BLOCK", "JIRIBAM"]),
                    # Tamenglong - Noney
                    data["block_name"].isin(
                        ["HAOCHONG TD BLOCK", "KHOUPUM", "LONGMAI TD BLOCK", "NUNGBA"]
                    ),
                    # Thoubal - Kakching
                    data["block_name"].isin(["KAKCHING", "LANGMEIDONG CD BLOCK"]),
                ]

                options = [
                    "TENGNOUPAL",
                    "KAMJONG",
                    "PHERZAWL",
                    "KANGPOKPI",
                    "JIRIBAM",
                    "NONEY",
                    "KAKCHING",
                ]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            elif file.stem == "MADHYA PRADESH":
                # for districts in Madhya Pradesh
                conditions = [
                    # TIKAMGARH - Niwari
                    data["block_name"].isin(["NIWARI", "PRITHVIPUR"])
                ]

                options = ["NIWARI"]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            elif file.stem == "MIZORAM":
                # for districts in Mizoram
                conditions = [
                    # Aizawl - Saitual
                    # Chaphai - Saiual
                    # Phullen is added in Aizawl and Ngopa is included in Champhai. Te district name has been corrected as Saitual
                    data["block_name"].isin(["PHULLEN", "NGOPA"]),
                    # Champhai - KHAWZAWL
                    data["block_name"].isin(["KHAWZAWL"]),
                    # Lungeli - Hnahthial
                    data["block_name"].isin(["HNAHTHIAL"]),
                ]

                options = ["SAITUAL", "KHAWZAWL", "HNAHTHIAL"]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            return data

        df = dist_bifur(df)

        def dist_name_rectify(data):

            if file.stem == "ARUNACHAL PRADESH":
                conditions = [
                    # for KAMLE district in Arunachal Pradesh
                    # Here Puchi Geko block is in Upper Subansiri, however its actually in Kamle district.Hence, this entry is not a district bifurcation but a name correction whose synatx works well within the district bifur function.
                    data["block_name"].isin(["PUCHIGEKU"])
                ]

                options = ["KAMLE"]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            elif file.stem == "ASSAM":
                conditions = [
                    # for Dhubri district in ASSAM
                    # south salmara is in South Salmara Mankachar district
                    data["block_name"].isin(["SOUTH SALMARA"])
                ]

                options = ["SOUTH SALMARA-MANKACHAR"]
                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            elif file.stem == "RAJASTHAN":
                conditions = [
                    # for SRI GANGANAGAR district in Rajasthan
                    # rectifying the name of this district to GANGANAGAR
                    data["district"].isin(["SRI GANGANAGAR"])
                ]

                options = ["GANGANAGAR"]
                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            return data

        df = dist_name_rectify(df)

        #######################     MANUAL MAPPING LEVEL 3 #######################################################
        # function to manually map block names
        def block_mapper(data):
            # Andaman and Nicobar Islands
            if file.stem == "ANDAMAN AND NICOBAR":
                data["block_name"] = data["block_name"].str.replace(
                    "NANCOWRY", "NANCOWRIE"
                )
                data["block_name"] = data["block_name"].str.replace(
                    "NICOBAR", "CAR NICOBAR"
                )

            # Andhra Pradesh
            elif file.stem == "ANDHRA PRADESH":
                conditions = [
                    (data["district"].isin(["SRIKAKULAM"]))
                    & (data["block_name"].isin(["LAXMINARSUPETA"])),
                    (data["district"].isin(["CHITTOOR"]))
                    & (data["block_name"].isin(["SANTHI PURAM"])),
                    (data["district"].isin(["Y.S.R."]))
                    & (data["block_name"].isin(["S.A.K.N. MANDAL"])),
                    (data["district"].isin(["SPSR NELLORE"]))
                    & (data["block_name"].isin(["NELLORE"])),
                    (data["district"].isin(["SPSR NELLORE"]))
                    & (data["block_name"].isin(["OJILI"])),
                ]

                options = [
                    "L.N PETA",
                    "SANTHIPURAM HO ARIMUTHANAPALLE",
                    "SRI AVADUTHA KASINAYANA",
                    "NELLORE RURAL",
                    "OZILI",
                ]
                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # Aruncahal Pradesh
            elif file.stem == "ARUNACHAL PRADESH":
                conditions = [
                    (data["district"].isin(["DIBANG VALLEY"]))
                    & (data["block_name"].isin(["ANINI-ALINYE-MIPI"]))
                ]

                options = ["ANINI-MEPI"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # Assam
            elif file.stem == "ASSAM":
                conditions = [
                    # correcting name of Rangia block in Kamrup district
                    (data["district"].isin(["KAMRUP"]))
                    & (data["block_name"].isin(["RANGIAPART"])),
                    # correcting name of BATABRABA (PART) block in Marigaon district
                    (data["district"].isin(["MORIGAON"]))
                    & (data["block_name"].isin(["BATADRABA  MORIGAON PART"])),
                    # correcting name of DOLONGGHAT (PART) block in Marigaon district
                    (data["district"].isin(["MORIGAON"]))
                    & (data["block_name"].isin(["DOLONGGHAT MORIGAON PART"])),
                    # correcting name of MAYANG block in Marigaon district
                    (data["district"].isin(["MORIGAON"]))
                    & (data["block_name"].isin(["MAYONG"])),
                    # correcting name of West Ahaipur block in Charaideo district
                    (data["district"].isin(["CHARAIDEO"]))
                    & (data["block_name"].isin(["PACHIM ABHAIPUR"])),
                ]

                options = [
                    "RANGIA",
                    "BATABRABA (PART)",
                    "DOLONGGHAT (PART)",
                    "MAYANG",
                    "WEST ABHAIPUR",
                ]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

                condition2 = [
                    # correcting the name of district Morigaon to Marigaon
                    data["district"].isin(["MORIGAON"])
                ]

                option2 = ["MARIGAON"]

                data["district"] = np.select(
                    condition2, option2, default=data["district"]
                )

            # GUJARAT
            elif file.stem == "GUJARAT":
                conditions = [
                    # correcting name of Detroj Rampur block in Ahmadabad district
                    (data["district"].isin(["AHMADABAD"]))
                    & (data["block_name"].isin(["DATROJ"]))
                ]

                options = ["DETROJ RAMPURA"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # HIMACHAL PRADESH
            elif file.stem == "HIMACHAL PRADESH":
                conditions = [
                    # correcting name of Shree naina devi block in bilaspur district
                    (data["district"].isin(["BILASPUR"]))
                    & (data["block_name"].isin(["SHRI NAINA DEVI JI AT SWARGHAT"]))
                ]

                options = ["SHREE NAINA DEVI"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # JAMMU & KASHMIR
            elif file.stem == "JAMMU AND KASHMIR":
                conditions = [
                    # correcting name of Purmandal Bari Brahamna block in Samba district
                    (data["district"].isin(["SAMBA"]))
                    & (data["block_name"].isin(["BARI BRAHMANA"])),
                    # correcting name of Hazratbal  in Srinagar district
                    (data["district"].isin(["SRINAGAR"]))
                    & (data["block_name"].isin(["HAZRATBAL"])),
                    # correcting name of Sherabad Khore  in Baramulla district
                    (data["district"].isin(["BARAMULLA"]))
                    & (data["block_name"].isin(["KHORE SHERABAD"])),
                ]

                options = ["PURMANDAL BARI BRAHAMNA", "SRINAGAR", "SHERABAD KHORE"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # JHARKHAND
            elif file.stem == "JHARKHAND":
                conditions = [
                    # correcting name of MEDININAGAR block in Palamu district
                    (data["district"].isin(["PALAMU"]))
                    & (data["block_name"].isin(["DALTONGANJ"]))
                ]

                options = ["MEDININAGAR"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # KARNATAKA
            elif file.stem == "KARNATAKA":
                # found a new set of observations belonging to Kurwai block but its in Madhya Pradesh. So removing this from Karnataka file
                data = data[~data["block_name"].isin(["KURWAI"])]

                # correcting name of Hubbali block in Dharwar district
                conditions = [
                    (data["district"].isin(["DHARWAR"]))
                    & (data["block_name"].isin(["HUBLI"]))
                ]

                options = ["HUBBALLI"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # MADHYA PRADESH
            elif file.stem == "MADHYA PRADESH":
                conditions = [
                    # correcting name of SAINKHEDA block in Narsinghpur district
                    (data["district"].isin(["NARSINGHPUR"]))
                    & (data["block_name"].isin(["SAIKHEDA GADARWARA"]))
                ]

                options = ["SAINKHEDA"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # RAJASTHAN
            elif file.stem == "RAJASTHAN":
                conditions = [
                    # correcting name of Kishangarh SILORA block in Ajmer district
                    (data["district"].isin(["AJMER"]))
                    & (data["block_name"].isin(["SILORA"])),
                    # correcting name of PIRAWA (SUNEL) block in JHALAWAR district
                    (data["district"].isin(["JHALAWAR"]))
                    & (data["block_name"].isin(["PIDAWA"])),
                    # correcting name of KHARCHI(MAR.JUN) block in Pali district
                    (data["district"].isin(["PALI"]))
                    & (data["block_name"].isin(["MARWAR JUNCTION"])),
                    # correcting name of BAGIDORA block in BANSWARA district
                    (data["district"].isin(["BANSWARA"]))
                    & (data["block_name"].isin(["BAGEEDAURA"])),
                    # correcting name of VIJAINAGAR block in SRI GANGANAGAR district
                    (data["district"].isin(["SRI GANGANAGAR"]))
                    & (data["block_name"].isin(["SRIVIJAYNAGR"])),
                    # correcting name of Baytoo block in BARMER district
                    (data["district"].isin(["BARMER"]))
                    & (data["block_name"].isin(["BAITU"])),
                ]

                options = [
                    "KISHANGARH SILORA",
                    "PIRAWA (SUNEL)",
                    "KHARCHI(MAR.JUN)",
                    "BAGIDORA",
                    "VIJAINAGAR",
                    "BAYTOO",
                ]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # SIKKIM
            elif file.stem == "SIKKIM":
                conditions = [
                    # correcting name of ARITHANG CHONGRANG block in West District
                    (data["district"].isin(["WEST DISTRICT"]))
                    & (data["block_name"].isin(["CHONGRANG"]))
                ]

                options = ["ARITHANG CHONGRANG"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # TAMIL NADU
            elif file.stem == "TAMIL NADU":
                conditions = [
                    # correcting name of RAJASINGAMANGALAM block in RAMANATHAPURAM District
                    (data["district"].isin(["RAMANATHAPURAM"]))
                    & (data["block_name"].isin(["R.S. MANGALAM"])),
                    # correcting name of PEDDANAICKENPALAYAM block in SALEM District
                    (data["district"].isin(["SALEM"]))
                    & (data["block_name"].isin(["P.N.PALAYAM"])),
                    # correcting name of MACDONALDS CHOULTRY block in SALEM District
                    (data["district"].isin(["SALEM"]))
                    & (data["block_name"].isin(["MAC. CHOULTRY"])),
                    # correcting name of PERIYANAYAKKANPALAYAM block in COIMBATORE District
                    (data["district"].isin(["COIMBATORE"]))
                    & (data["block_name"].isin(["P.N.PALAYAM"])),
                    # correcting name of SARCARSAMAKULAM block in COIMBATORE District
                    (data["district"].isin(["COIMBATORE"]))
                    & (data["block_name"].isin(["S.S.KULAM"])),
                    # correcting name of UDHAGAMANDALAM block in THE NILGIRIS District
                    (data["district"].isin(["THE NILGIRIS"]))
                    & (data["block_name"].isin(["UDHAGAI"])),
                    # correcting name of KAMBAM block in THENI District
                    (data["district"].isin(["THENI"]))
                    & (data["block_name"].isin(["CUMBUM"])),
                    # correcting name of KADAMALAIKUNDRU MYLADUMPARAI block in THENI District
                    (data["district"].isin(["THENI"]))
                    & (data["block_name"].isin(["K MYLADUMPARAI"])),
                    # correcting name of TATTAYYANGARPETTAI block in TIRUCHIRAPPALLI District
                    (data["district"].isin(["TIRUCHIRAPPALLI"]))
                    & (data["block_name"].isin(["T.PET"])),
                    # correcting name of TIRUTTURAIPPUNDI block in TIRUVARUR District
                    (data["district"].isin(["TIRUVARUR"]))
                    & (data["block_name"].isin(["THIRUTHURAIPOONDI"])),
                    # correcting name of THIRUVENNAINALLUR block in VILLUPURAM District
                    (data["district"].isin(["VILLUPURAM"]))
                    & (data["block_name"].isin(["T.V. NALLUR"])),
                    # correcting name of VATTALKUNDU block in DINDIGUL District
                    (data["district"].isin(["DINDIGUL"]))
                    & (data["block_name"].isin(["BATLAGUNDU"])),
                    # correcting name of SATYAMANGALAM block in ERODE District
                    (data["district"].isin(["ERODE"]))
                    & (data["block_name"].isin(["SATHY"])),
                    # correcting name of THOOCKANAICKENPALAIYAM block in ERODE District
                    (data["district"].isin(["ERODE"]))
                    & (data["block_name"].isin(["T.N. PALAYAM"])),
                    # correcting name of ST.THOMAS MOUNT block in CHENGALPATTU District
                    (data["district"].isin(["CHENGALPATTU"]))
                    & (data["block_name"].isin(["THOMAS MALAI"])),
                ]

                options = [
                    "RAJASINGAMANGALAM",
                    "PEDDANAICKENPALAYAM",
                    "MACDONALDS CHOULTRY",
                    "PERIYANAYAKKANPALAYAM",
                    "SARCARSAMAKULAM",
                    "UDHAGAMANDALAM",
                    "KAMBAM",
                    "KADAMALAIKUNDRU MYLADUMPARAI",
                    "TATTAYYANGARPETTAI",
                    "TIRUTTURAIPPUNDI",
                    "THIRUVENNAINALLUR",
                    "VATTALKUNDU",
                    "SATYAMANGALAM",
                    "THOOCKANAICKENPALAIYAM",
                    "ST.THOMAS MOUNT",
                ]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # TELANGANA
            elif file.stem == "TELANGANA":
                conditions = [
                    # correcting name of GUNDLAPALLY (DINDI) block in NALGONDA District
                    (data["district"].isin(["NALGONDA"]))
                    & (data["block_name"].isin(["GUNDLA PALLE"])),
                    # correcting name of LOKESWARAM block in NIRMAL District
                    (data["district"].isin(["NIRMAL"]))
                    & (data["block_name"].isin(["LOHESRA"])),
                    # correcting name of Kesavapatnam village as SHANKARAPATNAM in KARIMNAGAR District
                    (data["district"].isin(["KARIMNAGAR"]))
                    & (data["block_name"].isin(["KESAVAPATNAM"])),
                ]

                options = ["GUNDLAPALLY (DINDI)", "LOKESWARAM", "SHANKARAPATNAM"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # UTTAR PRADESH
            elif file.stem == "UTTAR PRADESH":
                conditions = [
                    # correcting name of NAGAR (CITY) block in MIRZAPUR District
                    (data["district"].isin(["MIRZAPUR"]))
                    & (data["block_name"].isin(["CITY NAGAR"]))
                ]

                options = ["NAGAR (CITY)"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            return data

        df = block_mapper(df)

        # function to select villages and map blocks
        def block_mapper2(data):
            # JAMMU AND KASHMIR
            if file.stem == "JAMMU AND KASHMIR":
                conditions = [
                    # assigning Rajnagar vllage to BUDHAL NEW block in RAJAURI district
                    (data["district"] == "RAJAURI")
                    & (data["block_name"] == "RAJNAGAR")
                ]

                options = ["BUDHAL NEW"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # MADHYA PRADESH
            elif file.stem == "MADHYA PRADESH":
                conditions = [
                    # assigning SATNA area to SOHAWAL block in SATNA district
                    (data["district"] == "SATNA")
                    & (data["block_name"] == "SATNA")
                ]

                options = ["SOHAWAL"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # MANIPUR
            elif file.stem == "MANIPUR":
                conditions = [
                    # assigning villages in Impahl east I to sawombng block in Imphal east district
                    (data["district"] == "IMPHAL EAST")
                    & (data["block_name"] == "IMPHAL EAST I"),
                    # assigning villages in Impahl east II to Keirao CD block in Imphal east district
                    (data["district"] == "IMPHAL EAST")
                    & (data["block_name"] == "IMPHAL EAST II"),
                    # assigning villages in Mao TD Block to songsong block in Senapati district
                    (data["district"] == "SENAPATI")
                    & (data["block_name"] == "MAO  TD BLOCK"),
                    # assigning villages in Impahl West I to HAORANGSABAL block in Imphal west district
                    (data["district"] == "IMPHAL WEST")
                    & (data["block_name"] == "IMPHAL WEST I"),
                    # assigning villages in Impahl West II to WANGOI block in Imphal west district
                    (data["district"] == "IMPHAL WEST")
                    & (data["block_name"] == "IMPHAL WEST II"),
                ]

                options = [
                    "SAWOMBUNG CD BLOCK",
                    "KEIRAO CD BLOCK",
                    "SONG SONG",
                    "HAORANGSABAL",
                    "WANGOI",
                ]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # SIKKIM
            elif file.stem == "SIKKIM":
                conditions = [
                    # assigning villages in Dzongu to Passingdang block in North district
                    (data["district"] == "NORTH DISTRICT")
                    & (data["block_name"] == "DZONGU")
                ]

                options = ["PASSINGDANG"]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            # TELANGANA
            elif file.stem == "TELANGANA":
                conditions = [
                    # assigning villages in Gundala to YADADRI BHUVANAGIRI district
                    # Gundala actually belongs to YADADRI BHUVANAGIRI and not Jangoan, so correcting the district name for those observations
                    (data["district"] == "JANGAON")
                    & (data["block_name"] == "GUNDALA")
                ]

                options = ["YADADRI BHUVANAGIRI"]

                data["district"] = np.select(
                    conditions, options, default=data["district"]
                )

            # UTTAR PRADESH
            elif file.stem == "UTTAR PRADESH":
                conditions = [
                    # assigning RICHA village to BHUTA block in BAREILLY district
                    (data["district"] == "BAREILLY") & (data["block_name"] == "RICHHA"),
                    # assigning KHERGARH village to HATHWANT block in FIROZABAD district
                    (data["district"] == "FIROZABAD")
                    & (data["block_name"] == "KHERGARH"),
                    # assigning DINGARPUR village to KUNDARKI block in MORADABAD district
                    (data["district"] == "MORADABAD")
                    & (data["block_name"] == "DINGARPUR"),
                    # assigning villages in Alao to JAGEER block in MAINPURI district
                    (data["district"] == "MAINPURI") & (data["block_name"] == "ALAO"),
                    # assigning villages in Baniya khera to BILARI block in MORADABAD district
                    (data["district"] == "MORADABAD")
                    & (data["block_name"] == "BANIYA KHERA"),
                    # assigning villages in Bilari to BANIYAKHERA block in SAMBHAL district
                    (data["district"] == "SAMBHAL") & (data["block_name"] == "BILARI"),
                    # assigning villages in Hastinganj to HARIYANGATANGANJ block in AYODHYA district
                    (data["district"] == "AYODHYA")
                    & (data["block_name"] == "HASTINGANJ"),
                ]

                options = [
                    "BHUTA",
                    "HATHWANT",
                    "KUNDARKI",
                    "JAGEER",
                    "BILARI",
                    "BANIYAKHERA",
                    "HARIYANGATANGANJ",
                ]

                data["block_name"] = np.select(
                    conditions, options, default=data["block_name"]
                )

            return data

        df = block_mapper2(df)

        # concatenating states, districts and blocks for states

        df["state_dist"] = df["state"] + df["district"]
        df["state_dist_block"] = df["state"] + df["district"] + df["block_name"]

        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd_state_filtered,
            how="outer",
            left_on="state_dist_block",
            right_on="state_dist_block",
            validate="m:1",
            indicator=True,
            suffixes=["_NREGA", "_LGD"],
        )

        # tabs=df1['_merge'].value_counts()

        # filtering unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            [
                "state_NREGA",
                "district_NREGA",
                "block_name_NREGA",
                "state_dist_NREGA",
                "state_dist_block",
            ]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist_block")

        # Fuzzywuzzy for mapping
        print("*******Initiating Fuzzy Mapping for ", file.stem, "********")

        # creating a list of matches for unmerged state_dist_block names
        result = [
            process.extractOne(i, lgd_state_filtered["state_dist_block"])
            for i in not_lgd_mapped["state_dist_block"]
        ]

        # converting and editing the fuzzy matches to a dataframe
        result = pd.DataFrame(result, columns=["match", "score", "id"])
        result.drop("id", axis=1, inplace=True)

        # creating a proxy dataframe with names of unmerged original name sof state_dist_block
        not_lgd_proxy_df = (
            pd.DataFrame(not_lgd_mapped["state_dist_block"], index=None)
            .reset_index()
            .drop("index", axis=1)
        )

        # creating a dataframe for fuzzy mapper
        mapper_df = pd.concat(
            [not_lgd_proxy_df, result],
            axis=1,
            ignore_index=True,
            names=["original", "match", "score"],
        )
        mapper_df = mapper_df[mapper_df[2] >= 90]

        # creating a dictionary for fuzzy mapper
        mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))

        # applying the mapper dictionary on the original processed state file to correct for fuzzy matched names
        df["state_dist_block"] = df["state_dist_block"].replace(mapper_dict)

        print(
            "*******Fuzzy Mapping for",
            file.stem,
            "has ended. Proceeding for second round of Data Merge*******",
        )

        # Second round of data merge with update fuzzy matched names along with exact original names in the state file
        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd_state_filtered,
            how="outer",
            left_on="state_dist_block",
            right_on="state_dist_block",
            validate="m:1",
            indicator=True,
            suffixes=["_NREGA", "_LGD"],
        )

        df1["_merge"].value_counts()

        # filtering fuzzy unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            [
                "state_NREGA",
                "district_NREGA",
                "block_name_NREGA",
                "state_dist_NREGA",
                "state_dist_block",
            ]
        ]

        # **not_lgd_mapped=not_lgd_mapped.drop_duplicates(subset='state_dist_block')

        # **not_lgd_list.append(not_lgd_mapped)

        # filtering fuzzy mapped observations
        # **lgd_mapped = df1[(df1["_merge"] == "both")][
        # **[
        # **"state_NREGA",
        # **"district_NREGA",
        # **"block_name_NREGA",
        # **"state_LGD",
        # **"district_LGD",
        # **"block_name_LGD",
        # **"state_dist_block",
        # **]
        # **]

        # **lgd_mapped=lgd_mapped.drop_duplicates(subset='state_dist_block')

        # **lgd_list.append(lgd_mapped)

        # filtering merged rows
        df1 = df1[(df1["_merge"] == "both")]

        # renaming columns before sorting the necessary columns
        df1 = df1.rename(
            columns={
                "St_LGD_code": "state_LGD_code",
                "Dt_LGD_code": "district_LGD_code",
                "Bk_LGD_code": "block_LGD_code",
                "state_LGD": "state",
                "district_LGD": "district",
                "block_name_LGD": "block_name",
            }
        )

        # function to sort and select necessary columns for final data set
        def column_sort(data):
            # setting arrangement of rows
            columns = [
                "state",
                "state_LGD_code",
                "district",
                "district_LGD_code",
                "block_name",
                "block_LGD_code",
                "panchayat_name",
                "work_code",
                "work_started_date",
                "work_physically_completed_date",
                "finished_when",
                "sanction_amount_in_lakh",
                "total_amount_paid_since_inception_in_lakh",
                "total_mandays",
                "no_of_units",
                "is_secure",
                "work_status",
                "master_work_category_name",
                "work_category_name",
                "work_start_fin_year",
            ]

            data = data[columns]
            return data

        df1 = column_sort(df1)

        # writing state file to lgd_mapped directory
        print("Exporting", file.stem, "as CSV to directory")
        df1.to_csv(str("./data/processed/block_lgd_mapped/" + file.name), index=False)

    # converting the list into a final dataframe
    # **not_lgd=pd.concat(not_lgd_list, axis=0)
    # **lgd_matched=pd.concat(lgd_list, axis=0)

    # **print("Exporting not_lgd and LGD matched as CSV file to the Interim sub directory")
    # **not_lgd.to_csv("data/interim/not_lgd_after_fuzzy_4.csv", index=False)
    # **lgd_matched.to_csv("data/interim/lgd_fuzzy_matched.csv", index=False)

    print("Looping has ended")

    print(
        "***********************************************************************************************************************"
    )


block_lgd_mapper(".")
