import datetime
import pandas as pd
from mongo_placer import pla, agg
from databases import *

def agg_data_fonte(datastart: datetime, lista_nazioni: list[str], fonte: str, hour_timedelta: int):
    """
    Funzione per aggregare i dati di una fonte in un determinato giorno e inserirli nel database, viene eseguita ogni fine estrazione dati dalle fonti. Esegue la agg con la data attuale e l'ora 00:00:00 per inserire le medie giornaliere,
    e poi esegue la agg con la data attuale e l'ora attuale meno hour_timedelta per inserire le medie orarie e minute.

    Args:
    datastart (datetime): The starting date and time to aggregate data.
    lista_nazioni (list[str]): The list of nations to aggregate data for.
    fonte (str): The source to aggregate data from.
    hour_timedelta (int): The number of hours to subtract from the current time to aggregate hourly and minute data.

    Returns:
    None
    """
    # richiamo la funzione per aggregare i dati specificando la data e ora attuale, la lista delle nazioni e la fonte
    result = agg(datastart.replace(hour=0, minute=0, second=0,
                 microsecond=0), "daily", 0, lista_nazioni, fonte)

    with open("log_agg_data_fonte.txt", "a") as f:
        
        f.write("Aggregazione giornaliera\n")
        f.write("Data: {}\n".format(datastart))
        f.write("Fonte: {}\n".format(fonte))
        f.write("Nazioni: {}\n".format(lista_nazioni))
        f.write("\n")

        #mettere anche result["daily"] in un file di log
        f.write(f'{len(result["daily"]["nazione"])}\n')
        f.write(f'{len(result["daily"]["regione"])}\n')
        f.write(f'{len(result["daily"]["provincia"])}\n')
        f.write(f'{len(result["daily"]["comune"])}\n')
        f.write(f'{len(result["daily"]["squareID"])}\n')

    
    stringa_1, stringa_2 = "", ""

    for key in result["daily"].keys():  # per ogni chiave del dizionario
        n_dati = len(result["daily"][key])  # calcolo il numero di dati
        # stampa di controllo
        stringa_1 = "{} - {}: #dati: {}".format(key, "daily", n_dati)
        if key == "nazione":  # se la chiave è nazione
            # inserisco i dati nel database
            nazdaily.insert_many(result["daily"][key])
        elif key == "regione":  # se la chiave è regione
            # inserisco i dati nel database
            regdaily.insert_many(result["daily"][key])
        elif key == "provincia":  # se la chiave è provincia
            # inserisco i dati nel database
            prodaily.insert_many(result["daily"][key])
        elif key == "comune":  # se la chiave è comune
            # inserisco i dati nel database
            comdaily.insert_many(result["daily"][key])
        elif key == "squareID":  # se la chiave è squareID
            # inserisco i dati nel database
            sqrdaily.insert_many(result["daily"][key])

    # richiamo la funzione per aggregare i dati specificando la data e ora attuale, la lista dei punti di Copernicus e la fonte
    result = agg(datastart.replace(hour=(datastart-datetime.timedelta(hours=hour_timedelta)
                                            ).hour, minute=0, second=0, microsecond=0), "hourly", hour_timedelta, lista_nazioni, fonte)

    with open("log_agg_data_fonte.txt", "a") as f:

        f.write("Aggregazione oraria e minuti\n")
        f.write("Data: {}\n".format(datastart))
        f.write("Fonte: {}\n".format(fonte))
        f.write("Nazioni: {}\n".format(lista_nazioni))
        f.write("\n")

        #mettere anche result["hourly"] e result["minute"] in un file di log
        f.write("Aggregazione oraria\n")
        f.write(f'{len(result['hourly']['nazione'])}\n')
        f.write(f'{len(result['hourly']['regione'])}\n')
        f.write(f'{len(result['hourly']['provincia'])}\n')
        f.write(f'{len(result['hourly']['comune'])}\n')
        f.write(f'{len(result['hourly']['squareID'])}\n')

        f.write("Aggregazione minuti\n")
        f.write(f'{len(result['minute']['comune'])}\n')
        f.write(f'{len(result['minute']['squareID'])}\n')
    

    for t in ["hourly", "minute"]:  # per ogni tipo di aggregazione
        for key in result[t].keys():  # per ogni chiave del dizionario
            n_dati = len(result[t][key])  # calcolo il numero di dati
            # stampa di controllo
            stringa_2 = "{} - {}: #dati: {}".format(key, t, n_dati)
            if t == "hourly":  # se l'aggregazione è oraria
                if key == "nazione":  # se la chiave è nazione
                    # inserisco i dati nel database
                    nazhourly.insert_many(result[t][key])
                elif key == "regione":  # se la chiave è regione
                    # inserisco i dati nel database
                    reghourly.insert_many(result[t][key])
                elif key == "provincia":  # se la chiave è provincia
                    # inserisco i dati nel database
                    prohourly.insert_many(result[t][key])
                elif key == "comune":  # se la chiave è comune
                    # inserisco i dati nel database
                    comhourly.insert_many(result[t][key])
                elif key == "squareID":  # se la chiave è squareID
                    # inserisco i dati nel database
                    sqrhourly.insert_many(result[t][key])
            elif t == "minute":  # se l'aggregazione è minute
                if key == "comune":  # se la chiave è comune
                    # inserisco i dati nel database
                    comminute.insert_many(result[t][key])
                elif key == "squareID":  # se la chiave è squareID
                    # inserisco i dati nel database
                    sqrminute.insert_many(result[t][key])

    return stringa_1, stringa_2

def invioMongo(lista, f):
    l = pla(f)
    for naz in l:
        if naz not in lista:
            lista.append(naz)

def controllo_pm(pm1: float, pm25: float, pm10: float):
    """
    This function checks the validity of the pm1, pm2.5 and pm10 values.
    If all values are valid, it returns a dictionary with the values.
    If any value is invalid, it returns an empty dictionary.

    Args:
    pm1 (float): The pm1 value to check.
    pm25 (float): The pm2.5 value to check.
    pm10 (float): The pm10 value to check.

    Returns:
    dict: A dictionary with the valid pm1, pm2.5 and pm10 values, or an empty dictionary if any value is invalid.
    """
    if pm1 != None: # se il valore è diverso da None
        pm1 = float(pm1) # converto il valore in float
    else: # se il valore è None
        pm1 = 0 # assegno il valore 0
    if pm25 != None: # se il valore è diverso da None
        pm25 = float(pm25) # converto il valore in float
    else: # se il valore è None
        pm25 = 0 # assegno il valore 0
    if pm10 != None: # se il valore è diverso da None
        pm10 = float(pm10) # converto il valore in float
    else: # se il valore è None
        pm10 = 0 # assegno il valore 0
    if pm1 < 1000 and pm25 < 1000 and pm10 < 1000: # se tutti i valori sono minori di 1000
        if pm1 > 0 and pm25 > 0 and pm10 > 0: # se tutti i valori sono maggiori di 0
            if pm1 <= pm25 and pm25 <= pm10: # se pm1 è minore o uguale a pm25 e pm25 è minore o uguale a pm10
                return {"pm1": pm1, "pm2_5": pm25, "pm10": pm10} # ritorno un dizionario con i valori
            else: # se pm1 non è minore o uguale a pm25 e pm25 non è minore o uguale a pm10
                return {} # ritorno un dizionario vuoto
        elif pm1 == 0 and pm25 > 0 and pm10 > 0: # se pm1 è uguale a 0 e pm25 e pm10 sono maggiori di 0
            if pm25 <= pm10: # se pm25 è minore o uguale a pm10
                return {"pm2_5": pm25, "pm10": pm10} # ritorno un dizionario con i valori
            else: # se pm25 non è minore o uguale a pm10
                return {}  # ritorno un dizionario vuoto
        elif pm1 > 0 and pm25 == 0 and pm10 > 0: # se pm1 è maggiore di 0 e pm25 è uguale a 0 e pm10 è maggiore di 0
            if pm1 <= pm10: # se pm1 è minore o uguale a pm10
                return {"pm1": pm1, "pm10": pm10} # ritorno un dizionario con i valori
            else: # se pm1 non è minore o uguale a pm10
                return {} # ritorno un dizionario vuoto
        elif pm1 > 0 and pm25 > 0 and pm10 == 0: # se pm1 è maggiore di 0 e pm25 è maggiore di 0 e pm10 è uguale a 0
            if pm1 <= pm25: # se pm1 è minore o uguale a pm25
                return {"pm1": pm1, "pm2_5": pm25} # ritorno un dizionario con i valori
            else: # se pm1 non è minore o uguale a pm25
                return {} # ritorno un dizionario vuoto
        else: # se almeno uno dei valori è uguale a 0
            return {} # ritorno un dizionario vuoto
    else: # se almeno uno dei valori è maggiore di 1000
        return {} # ritorno un dizionario vuoto

def controllo_inq_df(df: pd.DataFrame):
    """
    This function checks the validity of pollutant values in a dataframe.
    It returns a new dataframe with only the rows that contain valid values for all three parameters.

    Args:
    df (pandas.DataFrame): The dataframe to check.

    Returns:
    pandas.DataFrame: A new dataframe with only the rows that contain valid values for pollutant.
    """
    valid_rows = [] # creo una lista vuota
    for index, row in df.iterrows(): # per ogni riga del dataframe
        pm1, pm25, pm10, no2, so2, co, o3, temp, umidita = 0, 0, 0, 0, 0, 0, 0, 0, 0 # assegno il valore 0 a tutte le variabili
        if "pm1" in row and row["pm1"] < 1000:
            pm1 = row["pm1"] # assegno il valore di PM1 alla variabile pm1
        if "pm2_5" in row and row["pm2_5"] < 1000:
            pm25 = row["pm2_5"] # assegno il valore di PM2.5 alla variabile pm25
        if "pm10" in row and row["pm10"] < 1000:
            pm10 = row["pm10"] # assegno il valore di PM10 alla variabile pm10
        if "no2" in row:
            no2 = row["no2"] # assegno il valore di NO2 alla variabile no2
        if "so2" in row:
            so2 = row["so2"] # assegno il valore di SO2 alla variabile so2
        if "co" in row:
            co = row["co"] # assegno il valore di CO alla variabile co
        if "o3" in row:
            o3 = row["o3"] # assegno il valore di O3 alla variabile o3
        if "temperatura" in row:
            temp = row["temperatura"] # assegno il valore di temperatura alla variabile temp
        if "umidita" in row:
            umidita = row["umidita"] # assegno il valore di umidità alla variabile umidita
        diz = {} # creo un dizionario vuoto
        diz.update(controllo_pm(pm1, pm25, pm10)) # aggiungo i valori di pm_dict al dizionario
        diz.update(controllo_no2(no2)) # aggiungo i valori di no2_dict al dizionario
        diz.update(controllo_so2(so2)) # aggiungo i valori di so2_dict al dizionario
        diz.update(controllo_co(co)) # aggiungo i valori di co_dict al dizionario
        diz.update(controllo_o3(o3)) # aggiungo i valori di o3_dict al dizionario
        diz.update(controllo_temp(temp)) # aggiungo i valori di temp_dict al dizionario
        diz.update(controllo_hum(umidita)) # aggiungo i valori di hum_dict al dizionario

        #aggiungere le chiavi che mancano a diz
        for key in row.keys():
            if key not in diz and key not in ["pm1", "pm2_5", "pm10", "no2", "so2", "co", "o3", "temperatura", "umidita"]:
                diz[key] = row[key]
        valid_rows.append(diz) # aggiungo il dizionario alla lista
    if valid_rows: # se la lista non è vuota
        return pd.DataFrame(valid_rows) # ritorno un dataframe con i valori della lista
    else: # se la lista è vuota
        return pd.DataFrame() # ritorno un dataframe vuoto

def controllo_hum(hum: float):
    """
    Check if the humidity value is valid and return a dictionary with the humidity value if it is.

    Args:
        hum (float): The humidity value to check.

    Returns:
        dict: A dictionary with the humidity value if it is valid, otherwise an empty dictionary.
    """
    if hum != None: # se il valore è diverso da None
        hum = float(hum) # converto il valore in float
    else: # se il valore è None
        hum = 0 # assegno il valore 0
    if hum < 100 and hum > 0: # se il valore è compreso tra 0 e 100
        return {"umidita": hum} # ritorno un dizionario con il valore
    else: # se il valore non è compreso tra 0 e 100
        return {} # ritorno un dizionario vuoto

def controllo_temp(temp: float):
    """
    Check if the temperature value is valid and return a dictionary with the temperature value if it is.

    Args:
        temp (float): The temperature value to check.

    Returns:
        dict: A dictionary with the temperature value if it is valid, otherwise an empty dictionary.
    """
    if temp != None: # se il valore è diverso da None
        temp = float(temp) # converto il valore in float
    else: # se il valore è None
        temp = -100 # assegno il valore -100
    if temp < 80 and temp > -30: # se il valore è compreso tra -30 e 80
        return  {"temperatura": temp} # ritorno un dizionario con il valore
    else: # se il valore non è compreso tra -30 e 80
        return {} # ritorno un dizionario vuoto

def controllo_no2(no2: float):
    """
    Check if the NO2 value is valid and return a dictionary with the NO2 value if it is.

    Args:
        no2 (float): The NO2 value to check.

    Returns:
        dict: A dictionary with the NO2 value if it is valid, otherwise an empty dictionary.
    """
    if no2 != None: # se il valore è diverso da None
        no2 = float(no2) # converto il valore in float
    else: # se il valore è None
        no2 = 0 # assegno il valore 0
    if no2 < 1000 and no2 > 0: # se il valore è compreso tra 0 e 1000
        return {"no2": no2} # ritorno un dizionario con il valore
    else: # se il valore non è compreso tra 0 e 1000
        return {} # ritorno un dizionario vuoto

def controllo_o3(o3: float):
    """
    Check if the O3 value is valid and return a dictionary with the O3 value if it is.

    Args:
        o3 (float): The O3 value to check.

    Returns:
        dict: A dictionary with the O3 value if it is valid, otherwise an empty dictionary.
    """
    if o3 != None: # se il valore è diverso da None
        o3 = float(o3) # converto il valore in float
    else: # se il valore è None 
        o3 = 0  # assegno il valore 0
    if o3 < 1000 and o3 > 0: # se il valore è compreso tra 0 e 1000
        return {"o3": o3} # ritorno un dizionario con il valore
    else: # se il valore non è compreso tra 0 e 1000
        return {} # ritorno un dizionario vuoto

def controllo_co(co: float):
    """
    Check if the CO value is valid and return a dictionary with the CO value if it is.

    Args:
        co (float): The CO value to check.

    Returns:
        dict: A dictionary with the CO value if it is valid, otherwise an empty dictionary.
    """
    if co != None: # se il valore è diverso da None
        co = float(co) # converto il valore in float
    else: # se il valore è None
        co = 0 # assegno il valore 0
    if co < 1000 and co > 0: # se il valore è compreso tra 0 e 1000
        return {"co": co} # ritorno un dizionario con il valore
    else: # se il valore non è compreso tra 0 e 1000
        return {} # ritorno un dizionario vuoto

def controllo_so2(so2: float):
    """
    Check if the SO2 value is valid and return a dictionary with the SO2 value if it is.

    Args:
        so2 (float): The SO2 value to check.

    Returns:
        dict: A dictionary with the SO2 value if it is valid, otherwise an empty dictionary.
    """
    if so2 != None: # se il valore è diverso da None
        so2 = float(so2) # converto il valore in float
    else: # se il valore è None
        so2 = 0 # assegno il valore 0
    if so2 < 1500 and so2 > 0: # se il valore è compreso tra 0 e 1500
        return {"so2": so2} # ritorno un dizionario con il valore
    else: # se il valore non è compreso tra 0 e 1500
        return {} # ritorno un dizionario vuoto

def controllo_pressione(pressione: float):
    """
    Check if the pressure value is valid and return a dictionary with the pressure value if it is.

    Args:
        pressione (float): The pressure value to check.

    Returns:
        dict: A dictionary with the pressure value if it is valid, otherwise an empty dictionary.
    """
    if pressione != None: # se il valore è diverso da None
        pressione = float(pressione) # converto il valore in float
    else: # se il valore è None
        pressione = 0 # assegno il valore 0
    if pressione < 2000 and pressione > 400: # se il valore è compreso tra 400 e 2000
        return {"pressione": pressione} # ritorno un dizionario con il valore
    else: # se il valore non è compreso tra 400 e 2000
        return {} # ritorno un dizionario vuoto

def controllo_voc(voc: float):
    """
    Check if the VOC value is valid and return a dictionary with the VOC value if it is.

    Args:
        voc (float): The VOC value to check.

    Returns:
        dict: A dictionary with the VOC value if it is valid, otherwise an empty dictionary.
    """
    if voc != None: # se il valore è diverso da None
        voc = float(voc) # converto il valore in float
    else: # se il valore è None
        voc = 0 # assegno il valore 0
    if voc > 0: # se il valore è maggiore di 0
        return {"voc": voc} # ritorno un dizionario con il valore
    else:  # se il valore non è maggiore di 0
        return {} # ritorno un dizionario vuoto

def controllo_vento_int(vento_int: float):
    """
    Check if the wind intensity value is valid and return a dictionary with the wind intensity value if it is.

    Args:
        vento_int (float): The wind intensity value to check.

    Returns:
        dict: A dictionary with the wind intensity value if it is valid, otherwise an empty dictionary.
    """
    if vento_int != None: # se il valore è diverso da None
        vento_int = float(vento_int) # converto il valore in float
    else: # se il valore è None
        vento_int = 0 # assegno il valore 0
    if vento_int > 0: # se il valore è maggiore di 0
        return {"vento_int": vento_int} # ritorno un dizionario con il valore
    else: # se il valore non è maggiore di 0
        return {} # ritorno un dizionario vuoto