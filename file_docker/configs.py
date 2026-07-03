from pymongo import MongoClient

from settings import (
    MONGO_AUTH_DB,
    MONGO_DB_MS,
    MONGO_DB_SSDB,
    MONGO_HOST,
    MONGO_PASSWORD,
    MONGO_PORT,
    MONGO_USERNAME,
)

mongo_host = MONGO_HOST
mongo_port = MONGO_PORT
mongo_user = MONGO_USERNAME
mongo_password = MONGO_PASSWORD
mongo_db_MS = MONGO_DB_MS
mongo_db_SSDB = MONGO_DB_SSDB

connection_string_MS = (
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db_MS}?authSource={MONGO_AUTH_DB}"
)

connection_string_SSDB = (
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db_SSDB}?authSource={MONGO_AUTH_DB}"
)

connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource={MONGO_AUTH_DB}"

mongo_MS = MongoClient(connection_string_MS)[mongo_db_MS]
mongo_SSDB = MongoClient(connection_string_SSDB)[mongo_db_SSDB]
mongo_generico = MongoClient(connection_string)
