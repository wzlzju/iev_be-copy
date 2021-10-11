import pandas
import json
import os, csv

import sqlite3

import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

dbpath = "../db/iev.db"
datadirpath = "./PCAdata/"
countriespath = r"D:\phd1\IEV\code and data\IEV\cepii data\tmp\countries.json"
with open(countriespath, "r") as f:
    countries = json.load(f)

yy1 = 1995
yy2 = 2019

conn = sqlite3.connect(dbpath)

def getPCAsourcedata():
    cur = conn.cursor()
    res = {}
    for c in countries.keys():
        res[c] = {}
        res[c]["exp"] = [0]*10
        res[c]["imp"] = [0]*10
    MAX = len(res.keys())
    for yy in range(yy1, yy2+1):
        year = str(yy)
        for cat in range(10):
            exe = 'select i, sum("sum(v)") as v from n%s where k=%d group by i order by v desc' % (year, cat)
            cur.execute(exe)
            r = cur.fetchall()
            for idx, cr in enumerate(r):
                res[cr[0]]["exp"][cat] = MAX - idx
            exe = 'select j, sum("sum(v)") as v from n%s where k=%d group by j order by v desc' % (year, cat)
            cur.execute(exe)
            r = cur.fetchall()
            for idx, cr in enumerate(r):
                res[cr[0]]["imp"][cat] = MAX - idx
        with open(datadirpath+"PCAsourcedatay"+year+".json", "w") as f:
            json.dump(res, f)

def getPCAdata():
    for yy in range(yy1, yy2+1):
        year = str(yy)
        with open(datadirpath+"PCAsourcedatay"+year+".json", "r") as f:
            data = json.load(f)
        d = np.array([data[c]["exp"] + data[c]["imp"] for c in data.keys()])
        pca = PCA(n_components=2)
        r=pca.fit_transform(d)
        res = {c: list(r[i]) for i, c in enumerate(data.keys())}
        with open(datadirpath+"PCAdatay"+year+".json", "w") as f:
            json.dump(res, f)

def showPCAresult(yy):
    year = str(yy)
    with open(datadirpath + "PCAdatay" + year + ".json", "r") as f:
        data = json.load(f)
    d = np.array([data[c] for c in data.keys()])
    d = d.T

    x = d[0]
    y = d[1]


    names = np.array(list(countries.keys()))
    c = np.random.randint(1, 5, size=len(x))
    continent = list(set([v["continent"] for v in countries.values()]))
    continent2color = {c: i for i, c in enumerate(continent)}
    c = np.array([continent2color[countries[c]["continent"]] for c in data.keys()])

    norm = plt.Normalize(1, len(continent))
    cmap = plt.cm.RdYlGn

    fig, ax = plt.subplots()
    sc = plt.scatter(x, y, c=c, s=36, cmap=cmap, norm=norm)

    annot = ax.annotate("", xy=(0, 0), xytext=(20, 20), textcoords="offset points",
                        bbox=dict(boxstyle="round", fc="w"),
                        arrowprops=dict(arrowstyle="->"))
    annot.set_visible(False)

    def update_annot(ind):
        #print(ind)
        pos = sc.get_offsets()[ind["ind"][0]]
        annot.xy = pos
        text = "{}, {}".format(" ".join(list(map(str, ind["ind"]))),
                               " ".join([names[n] for n in ind["ind"]]))
        text = "\n".join([countries[list(data.keys())[int(cc)]]["country_name_abbreviation"] for cc in list(map(str, ind["ind"]))])
        annot.set_text(text)
        annot.get_bbox_patch().set_facecolor(cmap(norm(c[ind["ind"][0]])))
        #annot.get_bbox_patch().set_alpha(0.4)

    def hover(event):
        vis = annot.get_visible()
        if event.inaxes == ax:
            cont, ind = sc.contains(event)
            if cont:
                update_annot(ind)
                annot.set_visible(True)
                fig.canvas.draw_idle()
            else:
                if vis:
                    annot.set_visible(False)
                    fig.canvas.draw_idle()

    fig.canvas.mpl_connect("motion_notify_event", hover)

    plt.show()


if __name__ == "__main__":
    getPCAsourcedata()
    getPCAdata()
    showPCAresult(2019)