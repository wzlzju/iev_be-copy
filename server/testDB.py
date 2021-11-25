import pandas
import json
import os, csv

from config import *

from flask import Flask, request
import sqlite3

from copy import deepcopy
from config import *

import subprocess


# obtain the static global data (metadata)
S3path = "../../../dataProcessLS/S3.json"
HS92TOS3path = "../../../dataProcessLS/HS92TOS3.csv"
datadirpath = "../cepii data/BACI_HS92_V202102/"
countriespath = "../cepii data/tmp/countries.json"
productspath = "../cepii data/tmp/product.json"
s_countriespath = "./data/s_countries.json"
tmpdirpath = "../cepii data/tmp/"
ca2hspath = tmpdirpath + "ca2hs.json"

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
def hs2ca(hsid):
    hc = HS92TOS3.get(hsid, None)
    if hc:
        return hc[0]
    else:
        print("cannot find", hsid, "in HS92TOS3 in func hs2ca")
        return None
with open(ca2hspath, "r") as f:
    catohs = json.load(f)
def ca2hs(ca):
    return catohs[ca]
with open(countriespath, "r") as f:
    countries = json.load(f)
with open(s_countriespath, "r") as f:
    s_countries = json.load(f)["results"]
    s_countries_code = [i["code"] for i in s_countries]
with open(productspath, "r") as f:
    products = json.load(f)

conn = sqlite3.connect("../db/iev.db")

ts = " union all ".join(["select * from n%d" % (i) for i in range(1995,2020)])
exe = "create table N as %s" % (ts)
cur = conn.cursor()
cur.execute(exe)

# conn = sqlite3.connect("../db/iev.db")

#df = pandas.read_csv("../cepii data/BACI_HS92_V202102/BACI_HS92_Y1995_V202102.csv")
#df.to_sql('y1995', conn, if_exists='append', index=False)


# cur = conn.cursor()
#
# year = "2018"
# cats = ["0", "1"]

# subquery = "".join((" (select * from hs92y" + year + " where i in " + str(tuple(s_countries_code)) + " and j in " + str(tuple(s_countries_code)) + ") ").split("'"))
# exe = "select t,i,j,k,v in" + subquery + "where 1 "
# for cat in cats:
#     hstuple = "".join(('('+str(ca2hs(cat))[1:-1]+')').split("'"))
#

# .import 'D:/phd1/IEV/code and data/IEV/cepii data/BACI_HS92_V202102/NY2018.csv' NY2018

# for yy in range(int(yy1),int(yy2)+1):
#     year = str(yy)
#     print(year)
#     cur.execute("drop table N"+year)
#     cur.execute("create table N"+year+" as select t,i,j,k,sum(v),sum(q) from ny"+year+" group by t,i,j,k")

# catconditions = ["0", "1", "2"]
# catstr = "".join(("("+str(catconditions)[1:-1]+")").split("'"))
# subquery = ' (select t,i,j,sum("sum(v)") as v,sum("sum(q)") as q from n'+year+' where k in '+catstr+' group by t,i,j) '
# exe = "select t,i,j,max(v) from " + subquery + " group by j"


#exe = "update hs92y1995 as NY1995 set k = substr((select s3 from hs92tos3 where s3=k),1,1)"
#print(exe)
#cur.execute(exe)
#res = cur.fetchall()

#print(res)

#subprocess.call(["sqlite3", "../db/iev.db", ".mode tabs", ".import file.tsv table_name"])

# print("\033[0;31mabc\033[0m")