import numpy as np
import pandas as pd

# importing devanagiri script to make unicode dictionary
devan = pd.read_json(".\data\external\devanagiri.json")

key = [x for y in devan["devan_text"] for x in y]
value = [x.upper() for y in devan["unicode"] for x in y]

devan = pd.DataFrame(data=[key, value]).transpose()

# creating unicode dictionary
uni_dict = dict(zip(devan["1_x"], devan[0]))

# importing english phonetics for making devanagiri - english dictionary
english = pd.read_json(".\data\external\english.json")

key_eng = [x for y in english["devan_text"] for x in y]
key_eng = key_eng[0:76]

value_eng = [
    x.replace("[", "").replace("]", "").replace("aa", "a").replace("ii", "i")
    for y in english["english_phonetics"]
    for x in y
]
value_eng = value_eng[0:75]
value_eng.append(".")

english = pd.DataFrame(data=[key_eng, value_eng]).transpose()

devan = pd.merge(left=devan, right=english, how="outer", left_on=0, right_on=0)

# filling as many NAs in english script in devan from external web sources
conditions = [
    devan[0] == "ऄ",
    devan[0] == "ऌ",
    devan[0] == "ऍ",
    devan[0] == "ऎ",
    devan[0] == "ऑ",
    devan[0] == "ऒ",
    devan[0] == "ऩ",
    devan[0] == "ऱ",
    devan[0] == "ळ",
    devan[0] == "ऴ",
    devan[0] == " ",
    devan[0] == "ऻ",
    devan[0] == " ",
    devan[0] == "ऽ",
    devan[0] == "ॏ",
    devan[0] == "ॐ",
    devan[0] == "क़",
    devan[0] == "ख़",
    devan[0] == "ग़",
    devan[0] == "ज़",
    devan[0] == "ड़",
    devan[0] == "ढ़",
    devan[0] == "फ़",
    devan[0] == "य़",
    devan[0] == "ॡ",
    devan[0] == " ",
    devan[0] == " ",
    devan[0] == "।",
    devan[0] == "॥",
    devan[0] == "ॲ",
    devan[0] == "ॳ",
    devan[0] == "ॴ",
    devan[0] == "ॵ",
]

options = [
    "a",
    "l",
    "e",
    "e",
    "o",
    "o",
    "n",
    "r",
    "l",
    "l",
    "m",
    "a",
    "m",
    "a",
    "au",
    "om",
    "k",
    "kh",
    "g",
    "j",
    "d",
    "dh",
    "f",
    "y",
    "li",
    "li",
    "li",
    "।",
    "।।",
    "am",
    "a",
    "o",
    "au",
]

devan["1_y"] = np.select(conditions, options, default=devan["1_y"])

# exporting edited file CSV
devan.to_csv(".\data\external\devanagiri.csv", index=False)
