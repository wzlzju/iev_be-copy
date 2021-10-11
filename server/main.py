import json
import os, csv
import base64
from config import *
import math

from flask import Flask, request
import sqlite3

from copy import deepcopy
import colorama
from collections import Iterable

# obtain the static global data (metadata)
S3path = "./data/S3.json"
HS92TOS3path = "./data/HS92TOS3.csv"
datadirpath = "../cepii data/BACI_HS92_V202102/"
countriespath = "./data/countries.json"
productspath = "./data/product.json"
s_countriespath = "./data/s_countries.json"
categoriespath = "./data/categories.json"
tmpdirpath = "./data/"
ca2hspath = tmpdirpath + "ca2hs.json"
metacategorypath = "./data/metacategory.txt"
hs92tocapath = "./data/hs92toca.json"
dbpath = "../db/iev.db"
LDAdatadirpath = "../code/LDAdata/"
PCAdatadirpath = "../code/PCAdata/"
configfilepath = "./config.json"

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
    hc = hsid if len(hsid) == 6 else "0" + hsid
    hc = hc[0:2]
    hc = hs92toca.get(hc, None)
    if hc:
        return hc
    else:
        print("cannot find", hsid, "in hs92toca in func hs2ca")
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
with open(categoriespath, "r") as f:
    categories = json.load(f)

with open("./data/s_country_total_export.json", "r") as f:
    sct_export = json.load(f)
with open("./data/s_country_total_import.json", "r") as f:
    sct_import = json.load(f)
with open("./data/s_country_mutual_export.json", "r") as f:
    scm_export = json.load(f)
with open("./data/s_country_mutual_import.json", "r") as f:
    scm_import = json.load(f)

if os.path.exists(configfilepath):
    with open(configfilepath, "r") as f:
        configf = json.load(f)
    for K in configf.keys():
        locals()[K] = configf[K]


colorama.init()

# define the app and connect the database

app = Flask(__name__)
app.debug = DEBUG

# check_same_thread=False:
# do not check the thread issues, so that the main process can connect database only one time at the beginning
# for the concurrent requirement, use db_connection every request
#conn = sqlite3.connect(dbpath, check_same_thread=False)
def db_connection():
    return sqlite3.connect(dbpath)
def db_close(db):
    db.close()
conn = db_connection()
db_close(conn)

# add the CORS header
@app.after_request
def cors(environ):
    environ.headers['Access-Control-Allow-Origin']='*'
    environ.headers['Access-Control-Allow-Method']='*'
    environ.headers['Access-Control-Allow-Headers']='x-requested-with,content-type'
    if not QUIET:
        print("\033[0;32mAfter request\033[0m")
    return environ

@app.route('/test/', methods=['POST', 'GET'])
def indextest():
    if not QUIET:
        print(request.args)
    inputData = request.args.get('inputData', 'no para')
    data_c = getcontent(inputData)
    return data_c

def getcontent(p):
    data_t = [{'name':'a','quality':1}, {'name':'b','quality':10}, {'name':'c','quality':100}]
    try:
        p = int(p)
        result = data_t[p]
    except:
        result = {'name':'default'}
    return json.dumps(result)

def parse_category(categorys):
    category = eval(categorys)
    if isinstance(category, Iterable):
        category = [i if type(i) is str else str(i) for i in category]
    else:
        category = [category if type(category) is str else str(category)]
    if len(category) == 0:
        category = [str(i) for i in range(10)]
    return category

def parse_selectedCountry(countrys):
    country = eval(countrys)
    if type(country) is not int:
        raise Exception("selectedCountry must be an id. ")
    return str(country)

def parse_selectedCountries(twocountrys):
    twocountries = eval(twocountrys)
    if not isinstance(twocountries, Iterable):
        raise Exception("selectedCountries must be iterable or None. ")
    if len(twocountries) != 2:
        raise Exception("There are " + str(len(twocountries)) + " selected countries, but need 2. ")
    twocountries = [i if type(i) is str else str(i) for i in twocountries]
    return twocountries[0], twocountries[1]

@app.route('/iev/country_list', methods=['GET'])
def returns_countries():
    res = []
    for scc in s_countries_code:
        sc = {}
        sc["id"] = scc
        sc["name"] = countries[scc]["country_name_abbreviation"]
        sc["alpha2"] = countries[scc]["iso_2digit_alpha"]
        sc["alpha3"] = countries[scc]["iso_3digit_alpha"]
        with open("../images/national-flags/country_"+scc+".png", 'rb') as f:
            img = base64.b64encode(f.read()).decode()
        sc["image"] = "data:image/jpg;base64,"+img
        res.append(sc)
    return json.dumps(res)

@app.route('/iev/category_list', methods=['GET'])
def returns_categories():
    res = []
    for idx, cat in enumerate(categories):
        sc = {}
        sc["id"] = categories[idx]["id"]
        sc["name"] = categories[idx]["text"].split(" - ")[-1]
        with open("../images/industry/category_"+categories[idx]["id"]+".png", 'rb') as f:
            img = base64.b64encode(f.read()).decode()
        sc["image"] = "data:image/jpg;base64,"+img
        res.append(sc)
    return json.dumps(res)

@app.route('/iev/all_countries', methods=['GET'])
def returnallcountries():

    return json.dumps(countries)

@app.route('/iev/table', methods=['POST'])
def returntable():

    conn = db_connection()
    cur = conn.cursor()

    # get query conditions
    datastr = request.get_data().decode("utf-8")
    data = json.loads(datastr)
    query_conditions = data.get("query_conditions", "default")

    # parse query conditions
    qcs = [None] * len(query_conditions)
    t = []
    ijk = []
    v = []
    for idx, item in enumerate(query_conditions):
        try:
            yy = int(item)
            if yy >= int(yy1) and yy <= int(yy2):
                t.append(str(yy))
            elif yy >= 0:
                v.append(str(yy))
        except:
            try:
                val = float(item)
                if val >= 0:
                    v.append(str(val))
            except:
                ij = []
                cur.execute(
                    'select * from countrycodes '
                    'where country_name_abbreviation like "%' + item + '%" or country_name_full like "%' + item + '%";'
                )
                res = cur.fetchall()
                for resi in res:
                    ij.append(resi[0])
                k = []
                cur.execute(
                    'select * from productcodeshs92 '
                    'where description like "%' + item + '%";'
                )
                res = cur.fetchall()
                for resi in res:
                    k.append(resi[0])
                ijk.append(ij + k)

    # convert query conditions to SQL and execute SQL
    res = []
    if len(t) >= 2 or len(v) >= 2:
        return json.dumps([])
    if len(t) == 1:
        exe = 'select t,i,j,k,v from hs92y' + t[0] + ' where 1 '
        for keys in ijk:
            exe += ' and ( 0 '
            for key in keys:
                exe += ' or (i=' + key + ' or j=' + key + ' or k=' + key + ') '
            exe += ' ) '
        if len(v) == 1:
            exe += ' and (v=' + v[0] + ') '
        exe += ' limit ' + str(MAXQUERYAMOUNT)
        if not QUIET:
            print("SQL query:", exe)
        cur.execute(exe)
        res = cur.fetchall()
    elif len(t) == 0:
        res = []
        for y in range(int(yy1), int(yy2)+1):
            exe = 'select t,i,j,k,v from hs92y' + str(y) + ' where 1 '
            for keys in ijk:
                exe += ' and ( 0 '
                for key in keys:
                    exe += ' or (i=' + key + ' or j=' + key + ' or k=' + key + ') '
                exe += ' ) '
            if len(v) == 1:
                exe += ' and (v=' + v[0] + ') '
            exe += ' limit ' + str(MAXQUERYAMOUNT)
            if not QUIET:
                print("SQL query:", exe)
            cur.execute(exe)
            res += cur.fetchall()
            if len(res) >= MAXQUERYAMOUNT:
                res = res[:MAXQUERYAMOUNT]
                break

    # convert form of results
    # [('2019', '8', '446', '160100', '12.084')]
    #  =>
    # [
    # 	{
    # 		year: string,
    # 		importCountry: string,
    # 		exportCountry: string,
    # 		category: string,
    # 		amount: string
    # 	}
    # ]
    ret = [{
        'id': str(i),
        'year': d[0],
        'importCountry': countries[d[2]]["country_name_abbreviation"],
        'exportCountry': countries[d[1]]["country_name_abbreviation"],
        'product': products[d[3]],
        'industry_category': categories[int(hs2ca(d[3]))]["text"].split(" - ")[-1],
        'amount': d[4]
    } for i, d in enumerate(res)]

    db_close(conn)

    return json.dumps(ret)

@app.route('/iev/base_chart', methods=['GET'])
def returnmaptrix():

    conn = db_connection()
    cur = conn.cursor()

    # get query conditions
    year = request.args.get("year", None)
    categorys = request.args.get("category", None)
    if not year:
        year = DEFAULT["year"]
    if not categorys:
        categorys = DEFAULT["categorys"]
    texp = sct_export[year]
    timp = sct_import[year]
    mexp = scm_export[year]
    mimp = scm_import[year]

    # parse query conditions
    category = parse_category(categorys)

    # response
    # [
    # 	{
    # 		countryName: string, // 国家名称
    # 		imptotal: number, // 进口总量
    # 		exptotal: number // 出口总量,
    # 		explist: [] // 当前国家到其他14个国家的出口量
    # 	}
    # ]
    res = []
    for ci in s_countries_code:
        cres = {}
        cres["countryName"] = countries[ci]["country_name_abbreviation"]
        impt = 0.
        expt = 0.
        for cat in category:
            impt += timp[ci][cat]
            expt += texp[ci][cat]
        cres["imptotal"] = impt
        cres["exptotal"] = expt
        cres["explist"] = []
        for cj in s_countries_code:
            ctd = {}
            ctd["countryName"] = countries[cj]["country_name_abbreviation"]
            ctv = 0.
            for cat in category:
                ctv += mexp[ci][cj][cat]
            ctd["expvalue"] = ctv
            cres["explist"].append(ctd)
        res.append(cres)

    db_close(conn)

    return json.dumps(res)

@app.route('/iev/force_graph', methods=['GET'])
def returnforcegraph():

    conn = db_connection()
    cur = conn.cursor()

    # get query conditions
    year = request.args.get("year", None)
    categorys = request.args.get("category", None)
    if not year:
        year = DEFAULT["year"]
    if not categorys:
        categorys = DEFAULT["categorys"]
    texp = sct_export[year]
    timp = sct_import[year]
    mexp = scm_export[year]
    mimp = scm_import[year]

    # parse query conditions
    category = parse_category(categorys)

    # response
    # {
    # 	nodes: [
    # 		{
    # 	        index: number, //数组索引
    # 			id: string, // 国家id
    # 			name: string, // 国家名称
    # 			alpha2: string, // 简写
    # 			alpha3: string, // 简写
    # 			continent: string, // 所属大洲
    # 			expsum: number // 出口总量
    # 		}
    # 	],
    #   links: [
    #       {
    #           source: IGraphNode, // 出口国家对象
    #           target: IGraphNode, // 进口国家对象
    #           value: number // 出（进）口量
    #       }
    #   ]
    # }

    # assemble the query
    catstr = "".join(("(" + str(category)[1:-1] + ")").split("'"))
    subquery = ' (select t,i,j,sum("sum(v)") as v,sum("sum(q)") as q from n'+year+' where k in '+catstr+' group by t,i,j) '
    exe1 = "select t,i,j,max(v) from " + subquery + " group by j"
    exe2 = "select t,i,sum(v) from " + subquery + " group by i"

    # execute the query
    if not QUIET:
        print("SQL query:", exe1)
    cur.execute(exe1)
    res1 = cur.fetchall()
    if not QUIET:
        print("SQL query:", exe2)
    cur.execute(exe2)
    res2 = cur.fetchall()

    id2idx = {}
    idx2id = {}

    nodes = []
    links = []
    for idx, c in enumerate(res2):
        nodes.append({
            "index": idx,
            "id": c[1],
            "name": countries[c[1]]["country_name_abbreviation"],
            "alpha2": countries[c[1]]["iso_2digit_alpha"],
            "alpha3":countries[c[1]]["iso_3digit_alpha"],
            "continent": countries[c[1]]["continent"],
            "expsum": max(math.log(float(c[2])+1, 10)-6, 0)*20+max(math.log(float(c[2])+1, 10), 0)
        })
        id2idx[c[1]] = idx
        idx2id[idx] = c[1]

    for idx, c in enumerate(res1):
        if id2idx.get(c[2], None) is None:
            nodes.append({
                "index": len(nodes),
                "id": c[2],
                "name": countries[c[2]]["country_name_abbreviation"],
                "alpha2": countries[c[2]]["iso_2digit_alpha"],
                "alpha3": countries[c[2]]["iso_3digit_alpha"],
                "continent": countries[c[2]]["continent"],
                "expsum": 0.
            })
            id2idx[c[2]] = len(nodes)-1
            idx2id[len(nodes)-1] = c[2]
        links.append({
            "source": nodes[id2idx[c[1]]],
            "target": nodes[id2idx[c[2]]],
            "value": float(c[3])
        })

    db_close(conn)

    return json.dumps({
        "nodes": nodes,
        "links": links
    })

@app.route('/iev/donut_chart', methods=['GET'])
def returndonutchart():

    conn = db_connection()
    cur = conn.cursor()

    # get query conditions
    year = request.args.get("year", None)
    categorys = request.args.get("category", None)
    twocountrys = request.args.get("selectedCountries", None)
    if not year:
        year = DEFAULT["year"]
    if not categorys:
        categorys = DEFAULT["categorys"]
    if not twocountrys:
        twocountrys = DEFAULT["twocountrys"]
    texp = sct_export[year]

    # parse query conditions
    category = parse_category(categorys)
    try:
        c1, c2 = parse_selectedCountries(twocountrys)
    except Exception as e:
        return json.dumps({"error": str(e)})
    if c1 not in s_countries_code or c2 not in s_countries_code:
        return json.dumps({"error": "Selected countries are not in the %d specific sountries. " % (len(s_countries_code))})

    # response
    # [
    # 	{
    # 		country: string, // name
    #       type_id: string,
    #       type_name: string, // category
    #       value: number
    # 	}
    # ]
    texp = sct_export[year]
    res = []
    # c1
    for cat in category:
        res.append({
            "country": countries[c1]["country_name_abbreviation"],
            "type_id": categories[int(cat)]["id"],
            "type_name": categories[int(cat)]["text"].split(" - ")[-1],
            "value": texp[c1][cat]
        })
    # c2
    for cat in category:
        res.append({
            "country": countries[c2]["country_name_abbreviation"],
            "type_id": categories[int(cat)]["id"],
            "type_name": categories[int(cat)]["text"].split(" - ")[-1],
            "value": texp[c2][cat]
        })

    db_close(conn)

    return json.dumps(res)

@app.route('/iev/choropleth_map', methods=['GET'])
def returnchoroplethmap():

    conn = db_connection()
    cur = conn.cursor()

    # get query conditions
    year = request.args.get("year", None)
    categorys = request.args.get("category", None)
    twocountrys = request.args.get("selectedCountries", None)
    if not year:
        year = DEFAULT["year"]
    if not categorys:
        categorys = DEFAULT["categorys"]
    if not twocountrys:
        twocountrys = DEFAULT["twocountrys"]
    texp = sct_export[year]

    # parse query conditions
    category = parse_category(categorys)
    try:
        c1, c2 = parse_selectedCountries(twocountrys)
    except Exception as e:
        return json.dumps({"error": str(e)})
    if c1 not in list(countries.keys()) or c2 not in list(countries.keys()):
        return json.dumps({"error": "Selected countries' ids cannot be recognized. "})

    catstr = "".join(("(" + str(category)[1:-1] + ")").split("'"))
    subquery = ' (select t,i,j,sum("sum(v)") as v,sum("sum(q)") as q from n' + year + ' where k in ' + catstr + ' group by t,i,j) '
    T1 = " (select t,i,j,v,q from %s where i=%s) " % (subquery, c1)
    T2 = " (select t,i,j,v,q from %s where i=%s) " % (subquery, c2)
    exe = "select T1.t as t,T1.j as j,(T1.v-T2.v)/(T1.v+T2.v) as r from %s as T1 inner join %s as T2 on T1.j=T2.j" % (T1, T2)

    if not QUIET:
        print("SQL query:", exe)

    cur.execute(exe)
    res = cur.fetchall()
    # response
    # {
    # 	country_code <string>: rate <number>
    # }
    ret = {j:r for t, j, r in res}

    db_close(conn)

    return json.dumps(ret)

@app.route('/iev/timeline', methods=['GET'])
def returntimeline():

    conn = db_connection()
    cur = conn.cursor()

    # get query conditions
    categorys = request.args.get("category", None)
    twocountrys = request.args.get("selectedCountries", None)
    sortlists = request.args.get("sortlist", None)
    if not categorys:
        categorys = DEFAULT["categorys"]
    if not twocountrys:
        twocountrys = DEFAULT["twocountrys"]
    if not sortlists:
        sortlists = DEFAULT["sortlists"]
    if sortlists != "false":
        sortlist = True
    else:
        sortlist = False

    # parse query conditions
    category = parse_category(categorys)
    try:
        c1, c2 = parse_selectedCountries(twocountrys)
    except Exception as e:
        return json.dumps({"error": str(e)})
    if c1 not in list(countries.keys()) or c2 not in list(countries.keys()):
        return json.dumps({"error": "Selected countries' ids cannot be recognized. "})

    # results initialization
    res = {}
    reslen = {}
    for cid in countries.keys():
        res[cid] = []
        reslen[cid] = 0

    for i, yy in enumerate(range(int(yy1), int(yy2)+1)):
        year = str(yy)
        catstr = "".join(("(" + str(category)[1:-1] + ")").split("'"))
        subquery = ' (select t,i,j,sum("sum(v)") as v,sum("sum(q)") as q from n' + year + ' where k in ' + catstr + ' group by t,i,j) '
        T1 = " (select t,i,j,v,q from %s where i=%s) " % (subquery, c1)
        T2 = " (select t,i,j,v,q from %s where i=%s) " % (subquery, c2)
        exe = "select T1.t as t,T1.j as j,(T1.v-T2.v)/(T1.v+T2.v) as r from %s as T1 inner join %s as T2 on T1.j=T2.j" % (T1, T2)
        if not QUIET:
            print("SQL query:", exe)
        cur.execute(exe)
        cres = cur.fetchall()
        for t, j, r in cres:
            res[j].append(r)
            reslen[j] += 1
        for k in res.keys():
            if len(res[k]) < i+1:
                res[k].append(0.0)

    for k in reslen.keys():
        if reslen[k] <= (int(yy2)-int(yy1)+1)*DELRATE:
            del res[k]

    if sortlist:
        for i in range(len(res[list(res.keys())[0]])):
            tmp = []
            for c in res.keys():
                tmp.append(res[c][i])
            tmp.sort()
            for j, c in enumerate(list(res.keys())):
                res[c][i] = tmp[j]

    # response
    # {
    # 	country_code <string>: [rate <number>]
    # }

    db_close(conn)

    return json.dumps(res)

@app.route('/iev/stack_chart', methods=['GET'])
def returnstackchart():

    conn = db_connection()
    cur = conn.cursor()

    # get query conditions
    categorys = request.args.get("category", None)
    if not categorys:
        categorys = DEFAULT["categorys"]

    # parse query conditions
    category = parse_category(categorys)

    # response
    # [
    # 	{
    # 		date: number, // year
    #       BR: number,
    #       CN: number,
    #       ... ...
    #       US: number
    # 	}
    # ]
    res = {}
    res["data"] = []
    res["columns"] = ["date"]
    for yy in range(int(yy1), int(yy2)+1):
        year = str(yy)
        texp = sct_export[year]
        cres = {}
        cres["date"] = yy
        for ci in s_countries_code:
            alpha2 = countries[ci]["iso_2digit_alpha"]
            expt = 0.
            for cat in category:
                expt += texp[ci][cat]
            cres[alpha2] = expt
        res["data"].append(cres)
    for ci in s_countries_code:
        alpha2 = countries[ci]["iso_2digit_alpha"]
        res["columns"].append(alpha2)

    db_close(conn)

    return json.dumps(res)

def nametopic(y, tn=None):
    with open(LDAdatadirpath+"LDAdatay"+str(y)+".json", "r") as f:
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

@app.route('/iev/radar_chart', methods=['GET'])
def returnradarchart():

    # get query conditions
    year = request.args.get("year", None)
    countrys = request.args.get("selectedCountry", None)
    sorttopics = request.args.get("sorttopic", None)

    # parse query conditions
    if not year:
        year = DEFAULT["year"]
    if not countrys:
        countrys = DEFAULT["countrys"]
    if sorttopics != "true":
        sorttopic = False
    else:
        sorttopic = True

    try:
        c = parse_selectedCountry(countrys)
    except Exception as e:
        return json.dumps({"error": str(e)})
    if c not in list(countries.keys()):
        return json.dumps({"error": "Selected country is not in the country-ids. "})

    # response
    # [
    # 	{
    # 		axisname: string, // 轴名称
    # 		value: number // 数量
    # 	}
    # ]
    ret = []
    with open(LDAdatadirpath + "LDAdatay" + year + ".json", "r") as f:
        data = json.load(f)
    c_t = data["c_t_distribution"]
    t_v = data["t_v_distribution"]
    v = data["vocabulary"]
    td = list(enumerate(deepcopy(c_t[c])))
    topicnames = nametopic(int(year))
    if sorttopic:
        td.sort(key=lambda x: x[1], reverse=True)
    else:
        fixedtopicorder = ["High Class Products Export", "Low Class Products Export", "Resource Export", "Balance", "Industrial Products Import", "Daily Necessities Import"]
        td = [td[topicnames.index(fixedtopicorder[i])] for i in range(len(td))]
    for t, n in td:
        #tv = list(enumerate(deepcopy(t_v[t])))
        #tv.sort(key=lambda x: x[1], reverse=True)
        ret.append({
            #"axisname": v[tv[0][0]],
            "axisname": topicnames[t],
            "value": n
        })

    conn = db_connection()
    cur = conn.cursor()

    MAX = len(countries.keys())
    ret = []
    for cat in range(10):
        exe = 'select i, sum("sum(v)") as v from n%s where k=%d group by i order by v desc' % (year, cat)
        cur.execute(exe)
        r = cur.fetchall()
        for idx, cr in enumerate(r):
            if cr[0] == c:
                ret.append({
                    "axisname": categories[cat]["text"],
                    "value": MAX - idx
                })

    db_close(conn)


    res = {}
    res["data"] = ret
    res["range"] = len(countries.keys())

    return json.dumps(res)

@app.route('/iev/PCA_scatter', methods=['GET'])
def returnPCAscatter():

    # get query conditions
    year = request.args.get("year", None)

    # parse query conditions
    if not year:
        year = DEFAULT["year"]

    # response
    # [
    # 	{
    # 		id: string, // 国家id
    #       name: string, // 国家名称
    #       continent: string, // 所属大洲
    # 		x: number, // x
    # 		y: number // y
    # 	}
    # ]
    res = []
    with open(PCAdatadirpath + "PCAdatay" + year + ".json", "r") as f:
        data = json.load(f)

    for c in data.keys():
        res.append({
            "id": c,
            "name": countries[c]["country_name_abbreviation"],
            "continent": countries[c]["continent"],
            "x": data[c][0],
            "y": data[c][1]
        })

    return json.dumps(res)

if __name__ == '__main__':
    app.run(host=HOST, port=PORT)
