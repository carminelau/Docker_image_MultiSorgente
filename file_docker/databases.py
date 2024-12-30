from configs import mongo_MS, mongo_SSDB

historical=mongo_SSDB.historical_raw_data #raw data senza expire
raw=mongo_SSDB.raw_data
multipos=mongo_SSDB.multisorgente_position #informazioni geo-spaziali dati multisorgente
copernicus=mongo_SSDB.copernicus_position #informazioni geo-spaziali dati copernicus
sensorcommunity = mongo_SSDB.sensorcommunity_position #informazioni geo-spaziali dati sensorcommunity
citta_square = mongo_SSDB.citta_square #informazioni geo-spaziali dati square
windy_data = mongo_SSDB.windy_data #informazioni geo-spaziali dati windy

cache_openmeteo=mongo_SSDB.cache_openmeteo #cache dati openmeteo
cache_openmeteo_agg=mongo_SSDB.cache_openmeteo_agg #cache dati openmeteo aggregati
prev_openmeteo_giorno=mongo_SSDB.prev_openmeteo_giorno #previsioni openmeteo per giorno
prev_openmeteo_ora=mongo_SSDB.prev_openmeteo_ora #previsioni openmeteo per ora
import json
from time import strftime, strptime
import numpy as np
import requests
import pandas as pd
from multiprocessing import Pool
import datetime
import os
import sys
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/tools'
sys.path.append(path)
from mongo_placer import pla, agg
from databases import sensorcommunity,multipos, nazdaily, regdaily, prodaily, comdaily, sqrdaily, nazhourly, reghourly, prohourly, comhourly, sqrhourly, comminute, sqrminute
from Geobap import Geobap
from agg_data import controllo_inq_df, agg_data_fonte

global lista_naz  # da copiare
lista_naz = []  # da copiare

def invioMongo(lista, f):
    l = pla(f)
    for naz in l:
        if naz not in lista:
            lista.append(naz)

print("Inizio Sensor.Community: " + strftime("%Y-%m-%d %H:%M:%S", datetime.datetime.now().timetuple()))
start_time = datetime.datetime.now()
try:
    r_inq = requests.get("https://api.luftdaten.info/static/v2/data.dust.min.json")
    r_temp = requests.get("https://api.luftdaten.info/static/v2/data.temp.min.json")

    dati_inq = json.loads(r_inq.text)
    dati_temp = json.loads(r_temp.text)

    dataf_inq = pd.DataFrame(dati_inq)
    dataf_temp = pd.DataFrame(dati_temp)

    dataf_inq['latitude'] = dataf_inq.apply(lambda x: float(x['location']["latitude"]), axis=1)
    dataf_inq['longitude'] = dataf_inq.apply(lambda x: float(x['location']["longitude"]), axis=1)
    dataf_inq['pm10'] = dataf_inq.apply(lambda x: float(x['sensordatavalues'][0]["value"]) if x['sensordatavalues'][0]["value_type"] == 'P1' else 0, axis=1)
    dataf_inq['pm2_5'] = dataf_inq.apply(lambda x: float(x['sensordatavalues'][1]["value"]) if len(x['sensordatavalues'])> 1 else 0, axis=1)
    dataf_inq["ID"] = dataf_inq.apply(lambda x: "S.C" + str(x["id"]), axis=1)

    #creare una colonna timestamp con il timestamp in epoch
    dataf_inq['timestamp'] = dataf_inq.apply(lambda x: int(datetime.datetime.timestamp(datetime.datetime.strptime(x['timestamp'],'%Y-%m-%d %H:%M:%S').replace(tzinfo = datetime.timezone.utc)) + 3600), axis=1)

    #eliminare la colonna sensordatavalues, location, id, sensor
    dataf_inq = dataf_inq.drop(columns=['sensordatavalues', 'sampling_rate' ,'location', 'id', 'sensor'])

    dataf_temp['latitude'] = dataf_temp.apply(lambda x: float(x['location']["latitude"]), axis=1)
    dataf_temp['longitude'] = dataf_temp.apply(lambda x: float(x['location']["longitude"]), axis=1)
    dataf_temp['umidita'] = dataf_temp.apply(lambda x: float(x['sensordatavalues'][0]["value"]) if x['sensordatavalues'][0]["value_type"] == 'humidity' else 0, axis=1)
    dataf_temp['temperatura'] = dataf_temp.apply(lambda x: float(x['sensordatavalues'][1]["value"]) if len(x['sensordatavalues'])> 1 else 0, axis=1)
    dataf_temp["ID"] = dataf_temp.apply(lambda x: "S.C" + str(x["id"]), axis=1)

    #creare una colonna timestamp con il timestamp in epoch
    dataf_temp['timestamp'] = dataf_temp.apply(lambda x: int(datetime.datetime.timestamp(datetime.datetime.strptime(x['timestamp'],'%Y-%m-%d %H:%M:%S').replace(tzinfo = datetime.timezone.utc)) + 3600), axis=1)

    #eliminare la colonna sensordatavalues, location, id, sensor
    dataf_temp = dataf_temp.drop(columns=['sensordatavalues', 'sampling_rate' ,'location', 'id', 'sensor'])
    dataf = pd.merge(dataf_inq, dataf_temp, on=['timestamp', 'latitude', 'longitude'], how='outer')
    #se ID_x è NaN, allora ID = ID_y, se ID_y è NaN, allora ID = ID_x, se sono presenti entrambi, allora ID = ID_x

    dataf['ID'] = dataf['ID_x'].fillna(dataf['ID_y'])
    dataf = dataf.drop(['ID_x', 'ID_y'], axis=1)
    dataf["fonte"] = "SNSC"
    dataf["pm10"].replace(0, np.nan, inplace=True)
    dataf["pm2_5"].replace(0, np.nan, inplace=True)
    dataf["umidita"].replace(0, np.nan, inplace=True)

    dataf = dataf[['ID', 'timestamp', 'latitude', 'longitude', 'pm10', 'pm2_5', 'umidita', 'temperatura', 'fonte']]

    dataf = controllo_inq_df(dataf)

    position = pd.DataFrame(sensorcommunity.find({},{"_id":0}))
    final_df = pd.merge(dataf, position, on=['latitude', 'longitude'], how='outer')
    final_df.dropna(subset=["nazione", "regione", "provincia"], inplace=True)
    final_df.dropna(subset=["ID"], inplace=True)            
    final_df["timestamp"] = final_df["timestamp"].astype(int)

    json_array = json.loads(final_df.to_json(orient='records', default_handler=str))
    for item in json_array:
        keys_to_remove = []
        for key in item:
            if item[key] is None:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            item.pop(key)

    invioMongo(lista_naz, json_array)  # da copiare

    print(lista_naz)
    r = requests.get("http://188.166.29.27:5000/datatime")  # da copiare
    arr = r.text[1:-1].split(" ")  # da copiare
    data = datetime.datetime(int(arr[0]), int(arr[1]), int(
        arr[2]), int(arr[3]), int(arr[4]), int(arr[5]))  # da copiare
    agg_data_fonte(data, lista_naz, "SNSC", 1) # funzione per calcolare le medie giornaliere, orarie e al minuto e inviarle al db
    try:
        payload = {"apikey": "WDBNX4IUF66C", "processo": "SNSC_FULL", "delta": "1",
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "status": "200"}
        r = requests.post(
            "https://square.sensesquare.eu:5002/inserisci_status_processi", payload)
    except:
        print("Invio Status non riuscito")
        
except Exception as e:
    print(e)
    try:
        payload = {"apikey": "WDBNX4IUF66C", "processo": "SNSC_FULL", "delta": "1",
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "status": "500", "traceback": str(e)}
        r = requests.post(
            "https://square.sensesquare.eu:5002/inserisci_status_processi", payload)
    except:
        print("Invio Status non riuscito")import json
from time import strftime, strptime
import numpy as np
import requests
import pandas as pd
from multiprocessing import Pool
import datetime
import os
import sys
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/tools'
sys.path.append(path)
from mongo_placer import pla, agg
from databases import sensorcommunity,multipos, nazdaily, regdaily, prodaily, comdaily, sqrdaily, nazhourly, reghourly, prohourly, comhourly, sqrhourly, comminute, sqrminute
from Geobap import Geobap
from agg_data import controllo_inq_df, agg_data_fonte

global lista_naz  # da copiare
lista_naz = []  # da copiare

def invioMongo(lista, f):
    l = pla(f)
    for naz in l:
        if naz not in lista:
            lista.append(naz)

print("Inizio Sensor.Community: " + strftime("%Y-%m-%d %H:%M:%S", datetime.datetime.now().timetuple()))
start_time = datetime.datetime.now()
try:
    r_inq = requests.get("https://api.luftdaten.info/static/v2/data.dust.min.json")
    r_temp = requests.get("https://api.luftdaten.info/static/v2/data.temp.min.json")

    dati_inq = json.loads(r_inq.text)
    dati_temp = json.loads(r_temp.text)

    dataf_inq = pd.DataFrame(dati_inq)
    dataf_temp = pd.DataFrame(dati_temp)

    dataf_inq['latitude'] = dataf_inq.apply(lambda x: float(x['location']["latitude"]), axis=1)
    dataf_inq['longitude'] = dataf_inq.apply(lambda x: float(x['location']["longitude"]), axis=1)
    dataf_inq['pm10'] = dataf_inq.apply(lambda x: float(x['sensordatavalues'][0]["value"]) if x['sensordatavalues'][0]["value_type"] == 'P1' else 0, axis=1)
    dataf_inq['pm2_5'] = dataf_inq.apply(lambda x: float(x['sensordatavalues'][1]["value"]) if len(x['sensordatavalues'])> 1 else 0, axis=1)
    dataf_inq["ID"] = dataf_inq.apply(lambda x: "S.C" + str(x["id"]), axis=1)

    #creare una colonna timestamp con il timestamp in epoch
    dataf_inq['timestamp'] = dataf_inq.apply(lambda x: int(datetime.datetime.timestamp(datetime.datetime.strptime(x['timestamp'],'%Y-%m-%d %H:%M:%S').replace(tzinfo = datetime.timezone.utc)) + 3600), axis=1)

    #eliminare la colonna sensordatavalues, location, id, sensor
    dataf_inq = dataf_inq.drop(columns=['sensordatavalues', 'sampling_rate' ,'location', 'id', 'sensor'])

    dataf_temp['latitude'] = dataf_temp.apply(lambda x: float(x['location']["latitude"]), axis=1)
    dataf_temp['longitude'] = dataf_temp.apply(lambda x: float(x['location']["longitude"]), axis=1)
    dataf_temp['umidita'] = dataf_temp.apply(lambda x: float(x['sensordatavalues'][0]["value"]) if x['sensordatavalues'][0]["value_type"] == 'humidity' else 0, axis=1)
    dataf_temp['temperatura'] = dataf_temp.apply(lambda x: float(x['sensordatavalues'][1]["value"]) if len(x['sensordatavalues'])> 1 else 0, axis=1)
    dataf_temp["ID"] = dataf_temp.apply(lambda x: "S.C" + str(x["id"]), axis=1)

    #creare una colonna timestamp con il timestamp in epoch
    dataf_temp['timestamp'] = dataf_temp.apply(lambda x: int(datetime.datetime.timestamp(datetime.datetime.strptime(x['timestamp'],'%Y-%m-%d %H:%M:%S').replace(tzinfo = datetime.timezone.utc)) + 3600), axis=1)

    #eliminare la colonna sensordatavalues, location, id, sensor
    dataf_temp = dataf_temp.drop(columns=['sensordatavalues', 'sampling_rate' ,'location', 'id', 'sensor'])
    dataf = pd.merge(dataf_inq, dataf_temp, on=['timestamp', 'latitude', 'longitude'], how='outer')
    #se ID_x è NaN, allora ID = ID_y, se ID_y è NaN, allora ID = ID_x, se sono presenti entrambi, allora ID = ID_x

    dataf['ID'] = dataf['ID_x'].fillna(dataf['ID_y'])
    dataf = dataf.drop(['ID_x', 'ID_y'], axis=1)
    dataf["fonte"] = "SNSC"
    dataf["pm10"].replace(0, np.nan, inplace=True)
    dataf["pm2_5"].replace(0, np.nan, inplace=True)
    dataf["umidita"].replace(0, np.nan, inplace=True)

    dataf = dataf[['ID', 'timestamp', 'latitude', 'longitude', 'pm10', 'pm2_5', 'umidita', 'temperatura', 'fonte']]

    dataf = controllo_inq_df(dataf)

    position = pd.DataFrame(sensorcommunity.find({},{"_id":0}))
    final_df = pd.merge(dataf, position, on=['latitude', 'longitude'], how='outer')
    final_df.dropna(subset=["nazione", "regione", "provincia"], inplace=True)
    final_df.dropna(subset=["ID"], inplace=True)            
    final_df["timestamp"] = final_df["timestamp"].astype(int)

    json_array = json.loads(final_df.to_json(orient='records', default_handler=str))
    for item in json_array:
        keys_to_remove = []
        for key in item:
            if item[key] is None:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            item.pop(key)

    invioMongo(lista_naz, json_array)  # da copiare

    print(lista_naz)
    r = requests.get("http://188.166.29.27:5000/datatime")  # da copiare
    arr = r.text[1:-1].split(" ")  # da copiare
    data = datetime.datetime(int(arr[0]), int(arr[1]), int(
        arr[2]), int(arr[3]), int(arr[4]), int(arr[5]))  # da copiare
    agg_data_fonte(data, lista_naz, "SNSC", 1) # funzione per calcolare le medie giornaliere, orarie e al minuto e inviarle al db
    try:
        payload = {"apikey": "WDBNX4IUF66C", "processo": "SNSC_FULL", "delta": "1",
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "status": "200"}
        r = requests.post(
            "https://square.sensesquare.eu:5002/inserisci_status_processi", payload)
    except:
        print("Invio Status non riuscito")
        
except Exception as e:
    print(e)
    try:
        payload = {"apikey": "WDBNX4IUF66C", "processo": "SNSC_FULL", "delta": "1",
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), "status": "500", "traceback": str(e)}
        r = requests.post(
            "https://square.sensesquare.eu:5002/inserisci_status_processi", payload)
    except:
        print("Invio Status non riuscito")

print("Fine Sensor.Community: " + strftime("%Y-%m-%d %H:%M:%S", datetime.datetime.now().timetuple()))
print("Durata:" + str(datetime.datetime.now() - start_time))

print("Fine Sensor.Community: " + strftime("%Y-%m-%d %H:%M:%S", datetime.datetime.now().timetuple()))
print("Durata:" + str(datetime.datetime.now() - start_time))
cha=mongo_SSDB.centraline_hourly_avg #medie orarie per centraline
hcha=mongo_SSDB.historical_centraline_hourly_avg #medie orarie per centraline senza expire
cda=mongo_SSDB.centraline_daily_avg #medie giornaliere per centraline
hcda=mongo_SSDB.historical_centraline_daily_avg #medie giornaliere per centraline senza expire

muniha=mongo_SSDB.municipality_hourly_avg #medie orarie per comune
hmuniha=mongo_SSDB.historical_municipality_hourly_avg #medie orarie per comune senza expire
munida=mongo_SSDB.municipality_daily_avg #medie giornaliere per comune
hmunida=mongo_SSDB.historical_municipality_daily_avg #medie giornaliere per comune senza expire
munima=mongo_SSDB.municipality_minute_avg #medie per minuto per comune
hmunima=mongo_SSDB.historical_municipality_minute_avg #medie per minuto per comune senza expire
muniia=mongo_SSDB.municipality_instant_avg #medie 5 minuti precedenti per comune

kha=mongo_SSDB.square_hourly_avg #medie orarie per square
hkha=mongo_SSDB.historical_square_hourly_avg #medie orarie per square senza expire
kda=mongo_SSDB.square_daily_avg #medie giornaliere per square
hkda=mongo_SSDB.historical_square_daily_avg #medie giornaliere per square senza expire
kma=mongo_SSDB.square_minute_avg #medie per minuto per square
hkma=mongo_SSDB.historical_square_minute_avg #medie per minuto per square senza expire
kia=mongo_SSDB.square_instant_avg #medie 5 minuti precedenti per square

provha=mongo_SSDB.province_hourly_avg #medie orarie per provincia
hprovha=mongo_SSDB.historical_province_hourly_avg #medie orarie per provincia senza expire
provda=mongo_SSDB.province_daily_avg #medie giornaliere per provincia
hprovda=mongo_SSDB.historical_province_daily_avg #medie giornaliere per provincia senza expire
provia=mongo_SSDB.province_instant_avg #medie 5 minuti precedenti per provincia

regionha=mongo_SSDB.region_hourly_avg #medie orarie per regione
hregionha=mongo_SSDB.historical_region_hourly_avg #medie orarie per regione senza expire
regionda=mongo_SSDB.region_daily_avg #medie giornaliere per regione
hregionda=mongo_SSDB.historical_region_daily_avg #medie giornaliere per regione senza expire
regionia=mongo_SSDB.region_instant_avg #medie 5 minuti precedenti per regione

stateha=mongo_SSDB.state_hourly_avg #medie orarie per nazione
hstateha=mongo_SSDB.historical_state_hourly_avg #medie orarie per nazione senza expire
stateda=mongo_SSDB.state_daily_avg #medie giornaliere per nazione
hstateda=mongo_SSDB.historical_state_daily_avg #medie giornaliere per nazione senza expire
stateia=mongo_SSDB.state_instant_avg #medie 5 minuti precedenti per nazione

world=mongo_SSDB.world_geojson
region=mongo_SSDB.region_geojson
province=mongo_SSDB.province_geojson
municipality=mongo_SSDB.municipality_geojson
square=mongo_SSDB.square_geojson

world_hd=mongo_SSDB.world_geojson_hd
region_hd=mongo_SSDB.region_geojson_hd
province_hd=mongo_SSDB.province_geojson_hd
municipality_hd=mongo_SSDB.municipality_geojson_hd
square_hd=mongo_SSDB.square_geojson_hd

keyW=mongo_SSDB.sensors
keyR=mongo_SSDB.users

nazdaily=mongo_SSDB.state_daily_avg
regdaily=mongo_SSDB.region_daily_avg
prodaily=mongo_SSDB.province_daily_avg
comdaily=mongo_SSDB.municipality_daily_avg
sqrdaily=mongo_SSDB.square_daily_avg

nazhourly=mongo_SSDB.state_hourly_avg
reghourly=mongo_SSDB.region_hourly_avg
prohourly=mongo_SSDB.province_hourly_avg
comhourly=mongo_SSDB.municipality_hourly_avg
sqrhourly=mongo_SSDB.square_hourly_avg

comminute=mongo_SSDB.municipality_minute_avg
sqrminute=mongo_SSDB.square_minute_avg

msp_coll = mongo_MS.milano_smart_park

raw.create_index("timestamp", expireAfterSeconds=7776000)

cha.create_index("timestamp", expireAfterSeconds=31536000)
cda.create_index("timestamp", expireAfterSeconds=31536000)

kha.create_index("timestamp", expireAfterSeconds=31536000)
kda.create_index("timestamp", expireAfterSeconds=31536000)
kma.create_index("timestamp", expireAfterSeconds=7776000)
kia.create_index("timestamp", expireAfterSeconds=600)

muniha.create_index("timestamp", expireAfterSeconds=31536000)
munida.create_index("timestamp", expireAfterSeconds=31536000)
munima.create_index("timestamp", expireAfterSeconds=7776000)
muniia.create_index("timestamp", expireAfterSeconds=600)

provha.create_index("timestamp", expireAfterSeconds=31536000)
provda.create_index("timestamp", expireAfterSeconds=31536000)
provia.create_index("timestamp", expireAfterSeconds=600)

regionha.create_index("timestamp", expireAfterSeconds=31536000)
regionda.create_index("timestamp", expireAfterSeconds=31536000)
regionia.create_index("timestamp", expireAfterSeconds=600)

stateha.create_index("timestamp", expireAfterSeconds=31536000)
stateda.create_index("timestamp", expireAfterSeconds=31536000)
stateia.create_index("timestamp", expireAfterSeconds=600)

keyW.create_index("timestamp", expireAfterSeconds=31536000)
keyR.create_index("timestamp", expireAfterSeconds=31536000)