from configs import mongo

historical=mongo.historical_raw_data #raw data senza expire
raw=mongo.raw_data
multipos=mongo.multisorgente_position #informazioni geo-spaziali dati multisorgente
copernicus=mongo.copernicus_position #informazioni geo-spaziali dati copernicus
sensorcommunity = mongo.sensorcommunity_position #informazioni geo-spaziali dati sensorcommunity
citta_square = mongo.citta_square #informazioni geo-spaziali dati square
windy_data = mongo.windy_data #informazioni geo-spaziali dati windy

cache_openmeteo=mongo.cache_openmeteo #cache dati openmeteo
cache_openmeteo_agg=mongo.cache_openmeteo_agg #cache dati openmeteo aggregati
prev_openmeteo_giorno=mongo.prev_openmeteo_giorno #previsioni openmeteo per giorno
prev_openmeteo_ora=mongo.prev_openmeteo_ora #previsioni openmeteo per ora

cha=mongo.centraline_hourly_avg #medie orarie per centraline
hcha=mongo.historical_centraline_hourly_avg #medie orarie per centraline senza expire
cda=mongo.centraline_daily_avg #medie giornaliere per centraline
hcda=mongo.historical_centraline_daily_avg #medie giornaliere per centraline senza expire

muniha=mongo.municipality_hourly_avg #medie orarie per comune
hmuniha=mongo.historical_municipality_hourly_avg #medie orarie per comune senza expire
munida=mongo.municipality_daily_avg #medie giornaliere per comune
hmunida=mongo.historical_municipality_daily_avg #medie giornaliere per comune senza expire
munima=mongo.municipality_minute_avg #medie per minuto per comune
hmunima=mongo.historical_municipality_minute_avg #medie per minuto per comune senza expire
muniia=mongo.municipality_instant_avg #medie 5 minuti precedenti per comune

kha=mongo.square_hourly_avg #medie orarie per square
hkha=mongo.historical_square_hourly_avg #medie orarie per square senza expire
kda=mongo.square_daily_avg #medie giornaliere per square
hkda=mongo.historical_square_daily_avg #medie giornaliere per square senza expire
kma=mongo.square_minute_avg #medie per minuto per square
hkma=mongo.historical_square_minute_avg #medie per minuto per square senza expire
kia=mongo.square_instant_avg #medie 5 minuti precedenti per square

provha=mongo.province_hourly_avg #medie orarie per provincia
hprovha=mongo.historical_province_hourly_avg #medie orarie per provincia senza expire
provda=mongo.province_daily_avg #medie giornaliere per provincia
hprovda=mongo.historical_province_daily_avg #medie giornaliere per provincia senza expire
provia=mongo.province_instant_avg #medie 5 minuti precedenti per provincia

regionha=mongo.region_hourly_avg #medie orarie per regione
hregionha=mongo.historical_region_hourly_avg #medie orarie per regione senza expire
regionda=mongo.region_daily_avg #medie giornaliere per regione
hregionda=mongo.historical_region_daily_avg #medie giornaliere per regione senza expire
regionia=mongo.region_instant_avg #medie 5 minuti precedenti per regione

stateha=mongo.state_hourly_avg #medie orarie per nazione
hstateha=mongo.historical_state_hourly_avg #medie orarie per nazione senza expire
stateda=mongo.state_daily_avg #medie giornaliere per nazione
hstateda=mongo.historical_state_daily_avg #medie giornaliere per nazione senza expire
stateia=mongo.state_instant_avg #medie 5 minuti precedenti per nazione

world=mongo.world_geojson
region=mongo.region_geojson
province=mongo.province_geojson
municipality=mongo.municipality_geojson
square=mongo.square_geojson

world_hd=mongo.world_geojson_hd
region_hd=mongo.region_geojson_hd
province_hd=mongo.province_geojson_hd
municipality_hd=mongo.municipality_geojson_hd
square_hd=mongo.square_geojson_hd

keyW=mongo.sensors
keyR=mongo.users

nazdaily=mongo.state_daily_avg
regdaily=mongo.region_daily_avg
prodaily=mongo.province_daily_avg
comdaily=mongo.municipality_daily_avg
sqrdaily=mongo.square_daily_avg

nazhourly=mongo.state_hourly_avg
reghourly=mongo.region_hourly_avg
prohourly=mongo.province_hourly_avg
comhourly=mongo.municipality_hourly_avg
sqrhourly=mongo.square_hourly_avg

comminute=mongo.municipality_minute_avg
sqrminute=mongo.square_minute_avg

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