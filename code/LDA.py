import pandas
import json
import os, csv
from copy import deepcopy
import sqlite3

import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import matplotlib.pyplot as plt
import pyLDAvis.sklearn

dbpath = "../db/iev.db"
sdatadirpath = "./PCAdata/"
datadirpath = "./LDAdata/"
countriespath = r"D:\phd1\IEV\code and data\IEV\cepii data\tmp\countries.json"
categoriespath = r"D:\phd1\IEV\code and data\IEV\server\data\categories.json"

with open(categoriespath, "r") as f:
    categories = json.load(f)
with open(countriespath, "r") as f:
    countries = json.load(f)

yy1 = 1995
yy2 = 2019

def getLDAdata(n=10):
    for yy in range(int(yy1), int(yy2)+1):
        year = str(yy)
        print(year)
        with open(sdatadirpath+"PCAsourcedatay"+year+".json", "r") as f:
            sdata = json.load(f)
        data = []
        for cc in sdata.keys():
            data.append(" ".join([" ".join(["EXP" + categories[i]["text"]] * n) for i, n in enumerate(sdata[cc]["exp"])])
                        + " "
                        + " ".join([" ".join(["IMP" + categories[i]["text"]] * n) for i, n in enumerate(sdata[cc]["imp"])]))
        cntVector = CountVectorizer()
        cvecdata = cntVector.fit_transform(data)
        lda = LatentDirichletAllocation(n_components=n)
        ldares = lda.fit_transform(cvecdata)
        res = {}
        res["c_t_distribution"] = {list(sdata.keys())[i]: list(r) for i, r in enumerate(ldares)}
        res["t_v_distribution"] = [list(t) for t in lda.components_]
        res["vocabulary"] = cntVector.get_feature_names()
        with open(datadirpath+"LDAdatay"+year+".json", "w") as f:
            json.dump(res, f)

def showLDAcountry(c):
    for yy in range(int(yy1), int(yy2)+1):
        year = str(yy)
        with open(datadirpath+"LDAdatay"+year+".json", "r") as f:
            data = json.load(f)
        c_t = data["c_t_distribution"]
        t_v = data["t_v_distribution"]
        v = data["vocabulary"]
        td = list(enumerate(deepcopy(c_t[c])))
        td.sort(key=lambda x: x[1], reverse=True)
        t = td[0][0]
        tv = list(enumerate(deepcopy(t_v[t])))
        tv.sort(key=lambda x: x[1], reverse=True)
        s = year + ": " + " + ".join([v[vi] + " * " + str(vn) for vi, vn in tv])
        print(s)
    print("")
    for yy in range(int(yy1), int(yy2)+1):
        year = str(yy)
        with open(datadirpath+"LDAdatay"+year+".json", "r") as f:
            data = json.load(f)
        c_t = data["c_t_distribution"]
        t_v = data["t_v_distribution"]
        v = data["vocabulary"]
        td = list(enumerate(deepcopy(c_t[c])))
        td.sort(key=lambda x: x[1], reverse=True)
        s = year + ": "
        for t, n in td:
            tv = list(enumerate(deepcopy(t_v[t])))
            tv.sort(key=lambda x: x[1], reverse=True)
            s += v[tv[0][0]] + " * " + str(n) + " + "
        s = s[:-3]
        print(s)

def showtopic(y, tn=None):
    with open(datadirpath+"LDAdatay"+str(y)+".json", "r") as f:
        data = json.load(f)
    tv = data["t_v_distribution"]
    v = data["vocabulary"]
    topicnames = nametopic(y)
    if tn:
            ctv = list(enumerate(deepcopy(tv[tn])))
            ctv.sort(key=lambda x: x[1], reverse=True)
            print(topicnames[tn])
            print("\n".join([v[vi] + " * " + str(vn) for vi, vn in ctv]))
    else:
            for i in range(len(tv)):
                    ctv = list(enumerate(deepcopy(tv[i])))
                    ctv.sort(key=lambda x: x[1], reverse=True)
                    print(topicnames[i])
                    print("\n".join([v[vi] + " * " + str(vn) for vi, vn in ctv]))
                    print("\n")

def nametopic(y, tn=None):
    with open(datadirpath+"LDAdatay"+str(y)+".json", "r") as f:
        data = json.load(f)
    tv = data["t_v_distribution"]
    v = data["vocabulary"]
    ss = []
    for ti in range(len(tv)):
        s = {
            "exp": 0.,
            "imp": 0.,
            "resourceexp": 0.,
            "resourceimp": 0.,
            "raw-materialexp": 0.,
            "raw-materialimp": 0.,
            "daily-necessitiesexp": 0.,
            "daily-necessitiesimp": 0.,
            "industrialexp": 0.,
            "industrialimp": 0.,
            "low-class-manipulationexp": 0.,
            "low-class-manipulationimp": 0.,
            "high-class-manipulationexp": 0.,
            "high-class-manipulationimp": 0.,
            "otherexp": 0.,
            "otherimp": 0.
        }
        ctv = tv[ti]
        for i in range(len(ctv)):
            pre = v[i][0:3]
            post = v[i][3:]
            s[pre] += ctv[i]
            if post == "agriculture":
                s["daily-necessities"+pre] += ctv[i]
            elif post == "chemicals":
                s["industrial"+pre] += ctv[i]
                s["low-class-manipulation"+pre] += ctv[i]
                s["raw-material" + pre] += ctv[i]
            elif post == "electronics":
                s["industrial" + pre] += ctv[i]
                s["high-class-manipulation" + pre] += ctv[i]
            elif post == "machinery":
                s["industrial" + pre] += ctv[i]
                s["low-class-manipulation"+pre] += ctv[i]
                s["high-class-manipulation" + pre] += ctv[i]
            elif post == "metals":
                s["raw-material" + pre] += ctv[i]
            elif post == "minerals":
                s["resource" + pre] += ctv[i]
                s["raw-material" + pre] += ctv[i]
            elif post == "other":
                s["other"+pre] += ctv[i]
            elif post == "stone":
                s["resource" + pre] += ctv[i]
                s["raw-material" + pre] += ctv[i]
            elif post == "textiles":
                s["daily-necessities"+pre] += ctv[i]
                s["industrial"+pre] += ctv[i]
                s["low-class-manipulation"+pre] += ctv[i]
            elif post == "vehicles":
                s["industrial" + pre] += ctv[i]
                s["high-class-manipulation" + pre] += ctv[i]
        ss.append(s)
    topics = [""] * len(tv)
    def sargmax(ts):
        maxs = 0.
        maxi = -1
        for i in range(len(ss)):
            if ss[i].get(ts, 0.) > maxs:
                maxs = ss[i].get(ts, 0.)
                maxi = i
        return maxi
    topics[sargmax("resourceexp")] = "Resource Export"
    ss[sargmax("resourceexp")] = {}
    topics[sargmax("daily-necessitiesimp")] = "Daily Necessities Import"
    ss[sargmax("daily-necessitiesimp")] = {}
    topics[sargmax("high-class-manipulationexp")] = "High Class Products Export"
    ss[sargmax("high-class-manipulationexp")] = {}
    topics[sargmax("low-class-manipulationexp")] = "Low Class Products Export"
    ss[sargmax("low-class-manipulationexp")] = {}
    topics[sargmax("industrialimp")] = "Industrial Products Import"
    ss[sargmax("industrialimp")] = {}
    topics[sargmax("exp")] = "Balance"
    ss[sargmax("exp")] = {}
    if tn:
        return topics[tn]
    else:
        return topics

def showLDA(yy, n=6):
    year = str(yy)
    with open(sdatadirpath + "PCAsourcedatay" + year + ".json", "r") as f:
        sdata = json.load(f)
    data = []
    for cc in sdata.keys():
        data.append(" ".join([" ".join(["EXP" + categories[i]["text"]] * n) for i, n in enumerate(sdata[cc]["exp"])])
                    + " "
                    + " ".join([" ".join(["IMP" + categories[i]["text"]] * n) for i, n in enumerate(sdata[cc]["imp"])]))
    cntVector = CountVectorizer()
    cvecdata = cntVector.fit_transform(data)
    lda = LatentDirichletAllocation(n_components=n)
    ldares = lda.fit_transform(cvecdata)
    p = pyLDAvis.sklearn.prepare(lda, cvecdata, cntVector)
    pyLDAvis.save_html(p, 'lda.html')

def getLDAtest(n1=20, n2=6):
    for yy in range(int(yy2), int(yy2)+1):
        year = str(yy)
        print(year)
        with open(sdatadirpath+"PCAsourcedatay"+year+".json", "r") as f:
            sdata = json.load(f)
        data = []
        for cc in sdata.keys():
            data.append(" ".join([" ".join(["EXP" + categories[i]["text"]] * n) for i, n in enumerate(sdata[cc]["exp"])])
                        + " "
                        + " ".join([" ".join(["IMP" + categories[i]["text"]] * n) for i, n in enumerate(sdata[cc]["imp"])]))
        cntVector = CountVectorizer()
        cvecdata = cntVector.fit_transform(data)
        lda = LatentDirichletAllocation(n_components=n1)
        ldares = lda.fit_transform(cvecdata)
        res = {}
        res["c_t_distribution"] = {list(sdata.keys())[i]: list(r) for i, r in enumerate(ldares)}
        res["t_v_distribution"] = [list(t) for t in lda.components_]
        res["vocabulary"] = cntVector.get_feature_names()
        tr = [0.]*n1
        ct = res["c_t_distribution"]
        for c in ct.keys():
            for i, v in enumerate(ct[c]):
                tr[i] += v
        for i in range(len(tr)):
            tr[i] /= len(ct.keys())
        ft = list(enumerate(tr))
        ft.sort(key=lambda x: x[1], reverse=True)
        print(ft)

if __name__ == "__main__":
    #getLDAdata(6)
    #showLDAcountry("156")
    #showtopic(1995)
    #showLDA(2019, 20)
    getLDAtest(20,6)



