import copy
import datetime

def search(match, elem, coll, fonte):
    result = coll.aggregate([{ "$match": match}, { "$group": elem[0]}], allowDiskUse=True)
    result = [document for document in result]
    ret = []
    for doc in result:
        if elem[1][0] in doc["_id"].keys():
            for key in elem[1][1]:
                doc[key] = doc["_id"][key]
            doc["timestamp"] = datetime.datetime(1970,1,1,0,0,0)
            for key in elem[2]:
                if key == "year":
                    doc["timestamp"] = doc["timestamp"].replace(year=doc["_id"][key])
                elif key == "month":
                    doc["timestamp"] = doc["timestamp"].replace(month=doc["_id"][key])
                elif key == "day":
                    doc["timestamp"] = doc["timestamp"].replace(day=doc["_id"][key])
                elif key == "hour":
                    doc["timestamp"] = doc["timestamp"].replace(hour=doc["_id"][key])
                elif key == "minute":
                    doc["timestamp"] = doc["timestamp"].replace(minute=doc["_id"][key])
            doc["fonte"] = fonte
            del doc["_id"]
            del doc["count"]
            to_del = []
            for key in doc.keys():
                if doc[key] == 0:
                    to_del.append(key)
            for k in to_del:
                del doc[k]
            ret.append(doc)
    return ret

arr = ["co","direzione_vento","gas_resistance","intensita_vento","o3","pm1","pm10","pm2_5","pressione","sn","temperatura","tp","umidita","intesita_vento","voltage","cos","sin","aqi","no2","so2","c2h5oh","voc","h2s","traffico"]
group = {
    "_id": {
        "year": {
            "$year": {
                "$add": [
                    datetime.datetime(1970,1, 1, 0, 0, 0),
                    { "$multiply": [1000, "$timestamp"] }
                ]
            }
        },
        "month": {
            "$month": {
                "$add": [
                    datetime.datetime(1970,1, 1, 0, 0, 0),
                    { "$multiply": [1000, "$timestamp"] }
                ]
            }
        }, 
        "day": {
            "$dayOfMonth": {
                "$add": [
                    datetime.datetime(1970,1, 1, 0, 0, 0),
                    { "$multiply": [1000, "$timestamp"] }
                ]
            }
        }
    },
    "count": { "$sum": 1 }
}
for x in arr:
    group[x] = {
        "$sum": {
            "$cond": {
                "if": {"$gte": [ "${}".format(x), 0]},
                "then": "${}".format(x),
                "else": 0
    }}}
    group["count_{}".format(x)] = {
        "$sum": {
            "$cond": {
                "if": { "$and": [{"$ifNull": [ "${}".format(x), False ]},{"$ne": [ "${}".format(x), 0 ]}] },
                "then": 1,
                "else": 0
    }}}
group_nazioni_giorno = copy.deepcopy(group)
group_nazioni_giorno["_id"]["nazione"] = "$nazione"

group_nazioni_ora = copy.deepcopy(group_nazioni_giorno)
group_nazioni_ora["_id"]["hour"] = { "$hour": { "$add": [ datetime.datetime(1970,1, 1, 0, 0, 0), { "$multiply": [1000, "$timestamp"] } ] } }

group_regioni_giorno = copy.deepcopy(group_nazioni_giorno)
group_regioni_giorno["_id"]["regione"] = "$regione"

group_regioni_ora = copy.deepcopy(group_regioni_giorno)
group_regioni_ora["_id"]["hour"] = { "$hour": { "$add": [ datetime.datetime(1970,1, 1, 0, 0, 0), { "$multiply": [1000, "$timestamp"] } ] } }

group_province_giorno = copy.deepcopy(group_regioni_giorno)
group_province_giorno["_id"]["provincia"] = "$provincia"

group_province_ora = copy.deepcopy(group_province_giorno)
group_province_ora["_id"]["hour"] = { "$hour": { "$add": [ datetime.datetime(1970,1, 1, 0, 0, 0), { "$multiply": [1000, "$timestamp"] } ] } }

group_comuni_giorno = copy.deepcopy(group_province_giorno)
group_comuni_giorno["_id"]["comune"] = "$comune"

group_comuni_ora = copy.deepcopy(group_comuni_giorno)
group_comuni_ora["_id"]["hour"] = { "$hour": { "$add": [ datetime.datetime(1970,1, 1, 0, 0, 0), { "$multiply": [1000, "$timestamp"] } ] } }

group_comuni_minuti = copy.deepcopy(group_comuni_ora)
group_comuni_minuti["_id"]["minute"] = { "$minute": { "$add": [ datetime.datetime(1970,1, 1, 0, 0, 0), { "$multiply": [1000, "$timestamp"] } ] } }

group_square_giorno = copy.deepcopy(group_regioni_giorno)
group_square_giorno["_id"]["squareID"] = "$squareID"

group_square_ora = copy.deepcopy(group_square_giorno)
group_square_ora["_id"]["hour"] = { "$hour": { "$add": [ datetime.datetime(1970,1, 1, 0, 0, 0), { "$multiply": [1000, "$timestamp"] } ] } }

group_square_minuti = copy.deepcopy(group_square_ora)
group_square_minuti["_id"]["minute"] = { "$minute": { "$add": [ datetime.datetime(1970,1, 1, 0, 0, 0), { "$multiply": [1000, "$timestamp"] } ] } }
gruppi_dict = {
    "daily": [
        [[group_nazioni_giorno, ["nazione", ["nazione"]], ["year", "month", "day"], 0]],
        [[group_regioni_giorno, ["regione", ["nazione", "regione"]], ["year", "month", "day"], 1]],
        [[group_province_giorno, ["provincia", ["nazione", "regione", "provincia"]], ["year", "month", "day"], 2]],
        [[group_comuni_giorno, ["comune", ["nazione", "regione", "provincia", "comune"]], ["year", "month", "day"], 3]],
        [[group_square_giorno, ["squareID", ["nazione", "regione", "squareID"]], ["year", "month", "day"], 3]]
    ],
    "hourly": [
        [[group_nazioni_ora, ["nazione", ["nazione"]], ["year", "month", "day", "hour"], 0]],
        [[group_regioni_ora, ["regione", ["nazione", "regione"]], ["year", "month", "day", "hour"], 1]],
        [[group_province_ora, ["provincia", ["nazione", "regione", "provincia"]], ["year", "month", "day", "hour"], 2]],
        [[group_comuni_ora, ["comune", ["nazione", "regione", "provincia", "comune"]], ["year", "month", "day", "hour"], 3]],
        [[group_square_ora, ["squareID", ["nazione", "regione", "squareID"]], ["year", "month", "day", "hour"], 3]]
    ],
    "minute": [
        [[group_comuni_minuti, ["comune", ["nazione", "regione", "provincia", "comune"]], ["year", "month", "day", "hour", "minute"], 3]],
        [[group_square_minuti, ["squareID", ["nazione", "regione", "squareID"]], ["year", "month", "day", "hour", "minute"], 3]]
    ]
}
def aggregate(coll, fonte, t, start_date, hour_timedelta):
    d = {}
    #start_date = datetime.datetime(2021, 12, 1, 0, 0, 0)
    if t == "daily":
        end_date = (start_date + datetime.timedelta(days=1))
    else:
        end_date = (start_date + datetime.timedelta(hours=hour_timedelta))
    match = {"timestamp": {"$gte": int(start_date.timestamp()) + 3600, "$lt": int(end_date.timestamp()) + 3600}}
    gruppi = gruppi_dict[t]
    for gruppo in gruppi:
        arr = search(match, gruppo[0], coll, fonte)
        d[gruppo[0][1][0]] = arr
    return d
