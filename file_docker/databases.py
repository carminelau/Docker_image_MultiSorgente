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