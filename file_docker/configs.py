from pymongo import MongoClient

mongo_host = "mongo_multisorgente"
mongo_port = 27017
mongo_user = "multisorgente"
mongo_password = "multisorgente"
mongo_auth_db = "MS"

connection_string = (
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_auth_db}?authSource=admin"
)

mongo = MongoClient(connection_string)[mongo_auth_db]