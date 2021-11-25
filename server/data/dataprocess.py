import json, csv
from copy import deepcopy
import sqlite3
import pandas
import subprocess

yy1 = '1995'
yy2 = '2019'
S3path = "../../../../dataProcessLS/S3.json"
HS92TOS3path = "../../../../dataProcessLS/HS92TOS3.csv"
S3TOHS92path = "../../../../dataProcessLS/S3TOHS92.csv"
cepiidirpath = "../../cepii data/"
datadirpath = "../../cepii data/BACI_HS92_V202102/"
countriespath = "../../cepii data/tmp/countries.json"
tmpdirpath = "../../cepii data/tmp/"
ca2hspath = tmpdirpath + "ca2hs.json"
metacategorypath = "./metacategory.txt"
hs92tocapath = "./hs92toca.json"
sqlite3path = "D:/sqlite/sqlite3"
dbpath = "../../db/iev.db"

with open(S3path, "r") as f:
    S3 = json.load(f)
    catoparl = S3["results"]
catopard = {cato["id"]:cato['parent'] for cato in catoparl}
with open(HS92TOS3path, "r") as f:
    f_csv = csv.reader(f)
    header = None
    HS92TOS3 = {}
    for row in f_csv:
        if not header:
            header = deepcopy(row)
            continue
        HS92TOS3[row[0]] = row[1]
with open(hs92tocapath, "r") as f:
    hs92toca = json.load(f)
def hs2ca(hsid):
    hc = hsid if len(hsid) == 6 else "0"+hsid
    hc = hc[0:2]
    hc = hs92toca.get(hc, None)
    if hc:
        return hc
    else:
        print("cannot find", hsid, "in hs92toca in func hs2ca")
        return None
with open(S3TOHS92path, "r") as f:
    S3TOHS92 = json.load(f)
with open(countriespath, "r") as f:
    countries = json.load(f)
with open("./s_countries.json", "r") as f:
    s_countries = json.load(f)["results"]
    s_countries_code = [i["code"] for i in s_countries]
with open("./categories.json", "r") as f:
    categories = json.load(f)

def test():
    print(countries[list(countries.keys())[0]])
    print(S3["results"][7:17])
    with open(datadirpath+"BACI_HS92_Y1995_V202102.csv", "r") as f:
        f_csv = csv.reader(f)
        i = 0
        for row in f_csv:
            print(row)
            i += 1
            if i >= 10:
                break

def productcodes_csv2json():
    with open(cepiidirpath+"product_codes_HS92_V202102.csv", "r") as f:
        csv2json = {}
        header = None
        f_csv = csv.reader(f)
        for row in f_csv:
            if not header:
                header = deepcopy(row)
                continue
            csv2json[row[0]] = row[1]
    with open(tmpdirpath+"product.json", "w") as f:
        json.dump(csv2json, f)

def convertS3TOHS92():
    S3TOHS92 = {}
    for key in HS92TOS3.keys():
        if S3TOHS92.get(HS92TOS3[key], None):
            S3TOHS92[HS92TOS3[key]].append(key)
        else:
            S3TOHS92[HS92TOS3[key]] = [key]
    with open(S3TOHS92path, "w") as f:
        json.dump(S3TOHS92, f)

def getca2hs():
    with open(tmpdirpath+"product.json", "r") as f:
        hs92s = json.load(f)

    ca2hs = {}
    for i in range(10):
        ca2hs[str(i)] = []
    for key in hs92s.keys():
        ca2hs[hs2ca(key)].append(key if len(key) == 6 else "0"+key)
    with open(ca2hspath, "w") as f:
        json.dump(ca2hs, f)

def datacategoryconvert():
    for yy in range(int(yy1), int(yy2)+1):
        year = str(yy)
        print(year)
        print("\twriting")
        ftmp = open(datadirpath+"NY"+year+".csv", "w")
        with open(datadirpath+"BACI_HS92_Y"+year+"_V202102.csv", "r") as f:
            i = 0
            for line in f:
                if i>0:
                    row = line.split(",")
                    row[3] = hs2ca(row[3][1:-1])
                    line = ",".join(row)
                ftmp.write(line)
                i += 1

        ftmp.close()
        print("\timporting")
        subprocess.call([sqlite3path, dbpath, "drop table NY"+year+";"])
        subprocess.call([sqlite3path, dbpath, ".separator ','", ".import 'D:/phd1/IEV/code and data/IEV/cepii data/BACI_HS92_V202102/NY"+year+".csv' NY"+year])
        subprocess.call([sqlite3path, dbpath, "drop table N"+year+";"])
        subprocess.call([sqlite3path, dbpath, "create table N"+year+" as select t,i,j,k,sum(v),sum(q) from ny"+year+" group by t,i,j,k;"])


def Formaptrix():

    # data dict initialization
    resim = {}
    resex = {}
    # {"year": {"country_code": {"country_code": {"production_category": float}}}}      25year * 14country * 14country * 10category
    for year in range(int(yy1), int(yy2)+1):
        resim[str(year)] = {}
        resex[str(year)] = {}
        for s_c1 in s_countries_code:
            resim[str(year)][s_c1] = {}
            resex[str(year)][s_c1] = {}
            for s_c2 in s_countries_code:
                resim[str(year)][s_c1][s_c2] = {}
                resex[str(year)][s_c1][s_c2] = {}
                for cat_i in range(len(categories)):
                    resim[str(year)][s_c1][s_c2][str(cat_i)] = {"v": 0.0, "q": 0.0}
                    resex[str(year)][s_c1][s_c2][str(cat_i)] = {"v": 0.0, "q": 0.0}
    totalim = {}
    totalex = {}
    # {"year": {"country_code": {"production_code": float}}}
    for year in range(int(yy1), int(yy2)+1):
        totalim[str(year)] = {}
        totalex[str(year)] = {}
        for s_c in s_countries_code:
            totalim[str(year)][s_c] = {}
            totalex[str(year)][s_c] = {}
            for cat_i in range(len(categories)):
                totalim[str(year)][s_c][str(cat_i)] = {"v": 0.0, "q": 0.0}
                totalex[str(year)][s_c][str(cat_i)] = {"v": 0.0, "q": 0.0}
    for yy in range(int(yy1), int(yy2)+1):
        print(yy)
        with open(datadirpath + "BACI_HS92_Y"+str(yy)+"_V202102.csv", "r") as f:
            f_csv = csv.reader(f)
            header = None
            for row in f_csv:
                # header: ['t', 'i', 'j', 'k', 'v', 'q']
                if not header:
                    header = deepcopy(row)
                    continue
                # row: ['1995', '4', '12', '841510', '36.687', '5.812']
                if row[1] in s_countries_code and row[2] in s_countries_code:
                    hsid = row[3]
                    prodcat = hs2ca(hsid)
                    if prodcat:
                        resex[str(yy)][row[1]][row[2]][prodcat]["v"] += float(row[4])
                        resex[str(yy)][row[1]][row[2]][prodcat]["q"] += float(row[5] if row[5] != "" else "0")
                        resim[str(yy)][row[2]][row[1]][prodcat]["v"] += float(row[4])
                        resim[str(yy)][row[2]][row[1]][prodcat]["q"] += float(row[5] if row[5] != "" else "0")
                if row[1] in s_countries_code:
                    hsid = row[3]
                    prodcat = hs2ca(hsid)
                    if prodcat:
                        totalex[str(yy)][row[1]][prodcat]["v"] += float(row[4])
                        totalex[str(yy)][row[1]][prodcat]["q"] += float(row[5] if row[5] != "" else "0")
                if row[2] in s_countries_code:
                    hsid = row[3]
                    prodcat = hs2ca(hsid)
                    if prodcat:
                        totalim[str(yy)][row[2]][prodcat]["v"] += float(row[4])
                        totalim[str(yy)][row[2]][prodcat]["q"] += float(row[5] if row[5] != "" else "0")
    with open("./s_country_mutual_export.json", "w") as f:
        json.dump(resex, f)
    with open("./s_country_mutual_import.json", "w") as f:
        json.dump(resim, f)
    with open("./s_country_total_export.json", "w") as f:
        json.dump(totalex, f)
    with open("./s_country_total_import.json", "w") as f:
        json.dump(totalim, f)

def reruncategory():
    with open(metacategorypath, "r") as f:
        metacategory = json.load(f)
    categories = []
    hs92toca = {}
    for meta in metacategory:
        if meta["parent"] == "total":
            categories.append({
                "id": meta["id"],
                "text": meta["text"],
                "parent": "TOTAL"
            })
        else:
            hs92toca[meta["id"]] = str(meta["parent"])
    with open("./categories.json", "w") as f:
        json.dump(categories, f)
    with open("./hs92toca.json", "w") as f:
        json.dump(hs92toca, f)





if __name__ == "__main__":
    #test()
    #getca2hs()
    #datacategoryconvert()
    #convertS3TOHS92()
    #productcodes_csv2json()
    Formaptrix()
    #reruncategory()