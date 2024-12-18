from pymongo import MongoClient

mongo_host = "mongo_multisorgente"
mongo_port = 27017
mongo_user = "multisorgente"
mongo_password = "multisorgente"
mongo_db_MS = "MS"
mongo_db_SSDB = "SSDB"

connection_string_MS = (
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db_MS}?authSource=admin"
)

connection_string_SSDB = (
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db_SSDB}?authSource=admin"
)

connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"

mongo_MS = MongoClient(connection_string_MS)[mongo_db_MS]
mongo_SSDB = MongoClient(connection_string_SSDB)[mongo_db_SSDB]
mongo_generico = MongoClient(connection_string)