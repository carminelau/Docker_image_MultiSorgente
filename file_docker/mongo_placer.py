import pymongo
import datetime
import json
from aggregator import aggregate
from configs import mongo_generico

def __init__():
    print('ciao')

arpa_d = mongo_generico.arpa_daily
eea_d = mongo_generico.eea_daily
copernicus_d = mongo_generico.copernicus_daily
sensor_d = mongo_generico.sensor_daily
wlair_d = mongo_generico.wlair_daily
wise_d = mongo_generico.wise_daily
smpa_d = mongo_generico.smartpark.daily
here_d = mongo_generico.here_daily
arpa_h = mongo_generico.arpa_hourly
eea_h = mongo_generico.eea_hourly
wise_h = mongo_generico.wise_hourly
copernicus_h = mongo_generico.copernicus_hourly
sensor_h = mongo_generico.sensor_hourly
wlair_h = mongo_generico.wlair_hourly
smpa_h = mongo_generico.smartpark.hourly
here_h = mongo_generico.here_hourly

nazioni = ["Italia", "Germany", "Bulgaria", "France", "Austria", "United States", "Netherlands", "Poland", "Switzerland", "Hungary", "United Kingdom", "Belgium", "Czech Rep.", "Spain", "Portugal", "Romania", "Sweden", "Luxembourg", "Ireland", "Slovenia", "Slovakia", "India", "Macedonia", "Russia", "Mexico", "Thailand", "Finland", "Canada", "Ukraine", "Australia", "Serbia", "New Zealand", "Latvia", "Turkey", "Greece", "Bosnia and Herz.", "Norway", "Philippines", "Israel", "Vietnam", "China", "Argentina", "Pakistan", "Indonesia", "Singapore", "Kyrgyzstan", "Chile", "South Africa", "Kosovo", "Cyprus", "Lithuania", "Estonia", "Japan", "Korea", "Albania", "Belarus", "Croatia", "Colombia", "Brazil", "Denmark", "Armenia", "Moldova", "Kazakhstan", "Iceland", "Kenya", "Azerbaijan", "Nepal", "Niger", "Cambodia", "Andorra", "Gibraltar", "Taiwan", "Hong Kong", "Benin", "Italia-fake"]

def get_coll(db, coll):
    collections = { 
        "Aruba": db.Aruba,
        "Antigua and Barb.": db.Antigua_and_Barb,
        "Bajo Nuevo Bank (Petrel Is.)": db.Bajo_Nuevo_Bank_Petrel_Is,
        "St-Barthélemy": db.St_Barthélemy,
        "Belize": db.Belize,
        "Bermuda": db.Bermuda,
        "Bahamas": db.Bahamas,
        "Barbados": db.Barbados,
        "Anguilla": db.Anguilla,
        "Costa Rica": db.Costa_Rica,
        "Curaçao": db.Curaçao,
        "Cayman Is.": db.Cayman_Is,
        "Dominica": db.Dominica,
        "Dominican Rep.": db.Dominican_Rep,
        "Grenada": db.Grenada,
        "Guatemala": db.Guatemala,
        "Cuba": db.Cuba,
        "Greenland": db.Greenland,
        "Canada": db.Canada,
        "Honduras": db.Honduras,
        "Haiti": db.Haiti,
        "Jamaica": db.Jamaica,
        "St. Kitts and Nevis": db.St_Kitts_and_Nevis,
        "Saint Lucia": db.Saint_Lucia,
        "St-Martin": db.St_Martin,
        "Mexico": db.Mexico,
        "Montserrat": db.Montserrat,
        "Nicaragua": db.Nicaragua,
        "Puerto Rico": db.Puerto_Rico,
        "Panama": db.Panama,
        "Serranilla Bank": db.Serranilla_Bank,
        "El Salvador": db.El_Salvador,
        "St. Pierre and Miquelon": db.St_Pierre_and_Miquelon,
        "Sint Maarten": db.Sint_Maarten,
        "Turks and Caicos Is.": db.Turks_and_Caicos_Is,
        "Trinidad and Tobago": db.Trinidad_and_Tobago,
        "U.S. Minor Outlying Is.": db.US_Minor_Outlying_Is,
        "United States": db.United_States,
        "USNB Guantanamo Bay": db.USNB_Guantanamo_Bay,
        "St. Vin. and Gren.": db.St_Vin_and_Gren,
        "British Virgin Is.": db.British_Virgin_Is,
        "U.S. Virgin Is.": db.US_Virgin_Is,
        "Bolivia": db.Bolivia,
        "Argentina": db.Argentina,
        "Brazil": db.Brazil,
        "Colombia": db.Colombia,
        "Chile": db.Chile,
        "Ecuador": db.Ecuador,
        "Falkland Is.": db.Falkland_Is,
        "Peru": db.Peru,
        "Paraguay": db.Paraguay,
        "Guyana": db.Guyana,
        "Suriname": db.Suriname,
        "Uruguay": db.Uruguay,
        "Venezuela": db.Venezuela,
        "Angola": db.Angola,
        "Burundi": db.Burundi,
        "Benin": db.Benin,
        "Burkina Faso": db.Burkina_Faso,
        "Botswana": db.Botswana,
        "Côte d'Ivoire": db.Côte_dIvoire,
        "Central African Rep.": db.Central_African_Rep,
        "Cameroon": db.Cameroon,
        "Dem. Rep. Congo": db.Dem_Rep_Congo,
        "Congo": db.Congo,
        "Comoros": db.Comoros,
        "Cape Verde": db.Cape_Verde,
        "Djibouti": db.Djibouti,
        "Algeria": db.Algeria,
        "Egypt": db.Egypt,
        "Eritrea": db.Eritrea,
        "Ethiopia": db.Ethiopia,
        "Gabon": db.Gabon,
        "Ghana": db.Ghana,
        "Guinea": db.Guinea,
        "Gambia": db.Gambia,
        "Guinea-Bissau": db.Guinea_Bissau,
        "Eq. Guinea": db.Eq_Guinea,
        "Kenya": db.Kenya,
        "Liberia": db.Liberia,
        "Libya": db.Libya,
        "Lesotho": db.Lesotho,
        "Morocco": db.Morocco,
        "Madagascar": db.Madagascar,
        "Mali": db.Mali,
        "Mozambique": db.Mozambique,
        "Mauritania": db.Mauritania,
        "Malawi": db.Malawi,
        "Namibia": db.Namibia,
        "Niger": db.Niger,
        "Nigeria": db.Nigeria,
        "Rwanda": db.Rwanda,
        "W. Sahara": db.W_Sahara,
        "Sudan": db.Sudan,
        "S. Sudan": db.S_Sudan,
        "Senegal": db.Senegal,
        "Sierra Leone": db.Sierra_Leone,
        "Somaliland": db.Somaliland,
        "Somalia": db.Somalia,
        "São Tomé and Principe": db.São_Tomé_and_Principe,
        "Swaziland": db.Swaziland,
        "Chad": db.Chad,
        "Togo": db.Togo,
        "Tunisia": db.Tunisia,
        "Tanzania": db.Tanzania,
        "Uganda": db.Uganda,
        "South Africa": db.South_Africa,
        "Zambia": db.Zambia,
        "Zimbabwe": db.Zimbabwe,
        "Afghanistan": db.Afghanistan,
        "United Arab Emirates": db.United_Arab_Emirates,
        "Armenia": db.Armenia,
        "Azerbaijan": db.Azerbaijan,
        "Bangladesh": db.Bangladesh,
        "Bahrain": db.Bahrain,
        "Brunei": db.Brunei,
        "Bhutan": db.Bhutan,
        "China": db.China,
        "Cyprus U.N. Buffer Zone": db.Cyprus_UN_Buffer_Zone,
        "N. Cyprus": db.N_Cyprus,
        "Dhekelia": db.Dhekelia,
        "Cyprus": db.Cyprus,
        "Georgia": db.Georgia,
        "Hong Kong": db.Hong_Kong,
        "Indonesia": db.Indonesia,
        "India": db.India,
        "Indian Ocean Ter.": db.Indian_Ocean_Ter,
        "Iran": db.Iran,
        "Iraq": db.Iraq,
        "Israel": db.Israel,
        "Jordan": db.Jordan,
        "Japan": db.Japan,
        "Baikonur": db.Baikonur,
        "Siachen Glacier": db.Siachen_Glacier,
        "Kazakhstan": db.Kazakhstan,
        "Cambodia": db.Cambodia,
        "Kyrgyzstan": db.Kyrgyzstan,
        "Korea": db.Korea,
        "Kuwait": db.Kuwait,
        "Lao PDR": db.Lao_PDR,
        "Lebanon": db.Lebanon,
        "Sri Lanka": db.Sri_Lanka,
        "Macao": db.Macao,
        "Myanmar": db.Myanmar,
        "Mongolia": db.Mongolia,
        "Malaysia": db.Malaysia,
        "Nepal": db.Nepal,
        "Oman": db.Oman,
        "Pakistan": db.Pakistan,
        "Spratly Is.": db.Spratly_Is,
        "Philippines": db.Philippines,
        "Dem. Rep. Korea": db.Dem_Rep_Korea,
        "Palestine": db.Palestine,
        "Qatar": db.Qatar,
        "Saudi Arabia": db.Saudi_Arabia,
        "Syria": db.Syria,
        "Scarborough Reef": db.Scarborough_Reef,
        "Singapore": db.Singapore,
        "Thailand": db.Thailand,
        "Tajikistan": db.Tajikistan,
        "Turkmenistan": db.Turkmenistan,
        "Timor-Leste": db.Timor_Leste,
        "Taiwan": db.Taiwan,
        "Turkey": db.Turkey,
        "Uzbekistan": db.Uzbekistan,
        "Vietnam": db.Vietnam,
        "Akrotiri": db.Akrotiri,
        "Yemen": db.Yemen,
        "American Samoa": db.American_Samoa,
        "Ashmore and Cartier Is.": db.Ashmore_and_Cartier_Is,
        "Australia": db.Australia,
        "Cook Is.": db.Cook_Is,
        "Coral Sea Is.": db.Coral_Sea_Is,
        "Micronesia": db.Micronesia,
        "Fiji": db.Fiji,
        "Guam": db.Guam,
        "Kiribati": db.Kiribati,
        "Marshall Is.": db.Marshall_Is,
        "New Caledonia": db.New_Caledonia,
        "N. Mariana Is.": db.N_Mariana_Is,
        "Niue": db.Niue,
        "Norfolk Island": db.Norfolk_Island,
        "Nauru": db.Nauru,
        "Pitcairn Is.": db.Pitcairn_Is,
        "New Zealand": db.New_Zealand,
        "Palau": db.Palau,
        "Fr. Polynesia": db.Fr_Polynesia,
        "Papua New Guinea": db.Papua_New_Guinea,
        "Tonga": db.Tonga,
        "Tuvalu": db.Tuvalu,
        "Vanuatu": db.Vanuatu,
        "Solomon Is.": db.Solomon_Is,
        "Wallis and Futuna Is.": db.Wallis_and_Futuna_Is,
        "Samoa": db.Samoa,
        "Albania": db.Albania,
        "Aland": db.Aland,
        "Andorra": db.Andorra,
        "Austria": db.Austria,
        "Bosnia and Herz.": db.Bosnia_and_Herz,
        "Bulgaria": db.Bulgaria,
        "Belgium": db.Belgium,
        "Belarus": db.Belarus,
        "Switzerland": db.Switzerland,
        "Czech Rep.": db.Czech_Rep,
        "Germany": db.Germany,
        "Denmark": db.Denmark,
        "Spain": db.Spain,
        "Estonia": db.Estonia,
        "Faeroe Is.": db.Faeroe_Is,
        "Finland": db.Finland,
        "United Kingdom": db.United_Kingdom,
        "Guernsey": db.Guernsey,
        "Gibraltar": db.Gibraltar,
        "Greece": db.Greece,
        "Croatia": db.Croatia,
        "Hungary": db.Hungary,
        "Isle of Man": db.Isle_of_Man,
        "Ireland": db.Ireland,
        "Iceland": db.Iceland,
        "Italia": db.Italia,
        "Jersey": db.Jersey,
        "Kosovo": db.Kosovo,
        "Liechtenstein": db.Liechtenstein,
        "Lithuania": db.Lithuania,
        "Latvia": db.Latvia,
        "Monaco": db.Monaco,
        "Moldova": db.Moldova,
        "Macedonia": db.Macedonia,
        "Malta": db.Malta,
        "Montenegro": db.Montenegro,
        "Netherlands": db.Netherlands,
        "Poland": db.Poland,
        "Portugal": db.Portugal,
        "Norway": db.Norway,
        "Romania": db.Romania,
        "San Marino": db.San_Marino,
        "Russia": db.Russia,
        "Serbia": db.Serbia,
        "Slovakia": db.Slovakia,
        "Slovenia": db.Slovenia,
        "Luxembourg": db.Luxembourg,
        "Sweden": db.Sweden,
        "Ukraine": db.Ukraine,
        "Città del Vaticano": db.Città_del_Vaticano,
        "France": db.France,                                                                                                               
        "Italia-fake": db.Italia_fake  
    }
    return collections[coll]

def get_db(t, fonte):
    dbs = {
        "hourly": {
            "ARPA": arpa_h,
            "CP": copernicus_h,
            "EEA": eea_h,
            "WLAIR": wlair_h,
            "SNSC": sensor_h,
            "WISE": wise_h,
            "SMPA": smpa_h,
            "HERE": here_h
        },
        "daily": {
            "ARPA": arpa_d,
            "CP": copernicus_d,
            "EEA": eea_d,
            "WLAIR": wlair_d,
            "SNSC": sensor_d,
            "WISE": wise_d,
            "SMPA": smpa_d,
            "HERE": here_d
        }
    }

    return dbs[t][fonte]

def placer(arr):
    d = {}
    fonte = arr[0]["fonte"]
    flag = False
    for doc in arr:
        nazione = doc["nazione"]
        if flag == False:
            timestamp = datetime.datetime.utcfromtimestamp(doc["timestamp"])
            if 0 < timestamp.hour < 1:
                flag = True
        if nazione in d.keys():
            d[nazione].append(doc)
        else:
            d[nazione] = [doc]
    if flag:
        for t in ["daily", "hourly"]:
            db = get_db(t, fonte)
            mongo_generico.drop_database(db)
    else:
        db = get_db("hourly", fonte)
        mongo_generico.drop_database(db)
    for t in ["daily", "hourly"]:            
        for naz in d.keys():
            coll = get_coll(db, naz)
            coll.insert_many(d[naz])
    return list(d.keys())

def place_and_aggregate(arr):
    naz = placer(arr)
    fonte = arr[0]["fonte"]
    for t in ["daily", "hourly"]:
        for nazione in naz:
            coll = get_coll(get_db(t, fonte), nazione)
            result = aggregate(coll, fonte, t, None)
            with open("{}_{}.json".format(nazione, t), "w+") as fp:
                fp.write(json.dumps(result))

def pla(arr):
    d = {}
    fonte = arr[0]["fonte"]
    for doc in arr:
        nazione = doc["nazione"]
        if nazione in d.keys():
            d[nazione].append(doc)
        else:
            d[nazione] = [doc]
    for t in ["daily", "hourly"]:            
        for naz in d.keys():
            db = get_db(t, fonte)
            coll = get_coll(db, naz)
            coll.insert_many(d[naz])
    return list(d.keys())

def agg(start_date, frequency, hour_timedelta, naz, fonte):
    ret = {}
    if frequency == "daily":
        arr = ["daily"]
    else:
        arr = ["hourly", "minute"]
    for t in arr:
        diz = {"nazione": [], "regione": [], "provincia": [], "comune": [], "squareID": []}
        for nazione in naz:
            if t == "minute":
                tt = "hourly"
            else:
                tt = t
            coll = get_coll(get_db(tt, fonte), nazione)
            result = aggregate(coll, fonte, t, start_date, hour_timedelta)
            for r in result:
                diz[r].extend(result[r])
        to_del = []
        for key in diz:
            if len(diz[key]) == 0:
                to_del.append(key)
        for key in to_del:
            del diz[key]
        ret[t] = diz
    return ret