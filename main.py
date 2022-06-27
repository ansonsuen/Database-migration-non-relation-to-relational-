import json
import pandas as pd
import pymongo
import sqlalchemy
import certifi
from bson.json_util import dumps, loads
import numpy as np
from sqlalchemy import create_engine


def export_content():
    client = pymongo.MongoClient(
        "mongodb+srv://ansonsuen:ansonsuen@cluster0.hv6z9.mongodb.net/ex?retryWrites=true&w=majority",
        tlsCAFile=certifi.where())

    mydb = client["ex"]
    mycol = mydb['ex']
    cursor = mycol.find()
    list_cur = list(cursor)
    thejson = dumps(list_cur)
    client.close()
    with open('data.json', 'w', encoding='utf-8') as file:
        file.write(thejson)


def extract_kpos():
    # deal with nesting columns
    df = pd.read_json('data.json')
    json_struct = json.loads(df.to_json(orient="records"))
    df_flat = pd.json_normalize(json_struct)
    return df_flat


def transform():
    df = extract_kpos()
    df = df.rename(columns={'_id': 'id'})
    df = df.astype('string')
    df['kPos.qrCodeRefId'] = df['kPos.qrCodeRefId'].replace({None: 'null'})
    df = df.replace({'null': np.nan})

    df[['createdAt', 'updatedAt', 'kPos.transactedAt', 'kPos.startedAt', 'kPos.createdAt', 'expiryDate']] \
        = df[['createdAt', 'updatedAt', 'kPos.transactedAt', 'kPos.startedAt', 'kPos.createdAt', 'expiryDate']].apply(
        pd.to_datetime, errors='coerce')

    df[['kPos.discountTotal', 'kPos.paymentTotal', 'kPos.originalTotal', 'amount']] = \
        df[['kPos.discountTotal', 'kPos.paymentTotal', 'kPos.originalTotal', 'amount']].apply(pd.to_numeric,
                                                                                              errors='coerce')

    return df


def sqlcol(dfparam):
    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "string" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

        if "DateTime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict


def import_table():
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123',
        'database': 'ex'
    }
    db_user = config.get('user')
    db_pwd = config.get('password')
    db_host = config.get('host')
    db_port = config.get('port')
    db_name = config.get('database')
    engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_pwd}@{db_host}:{db_port}/ex')
    connection = engine.raw_connection()

    if connection.is_connected():
        data = transform()
        print(data.dtypes)

        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS ex.ex")
        #
        data.to_sql(name=db_name, con=engine, if_exists='replace', index=False, dtype=sqlcol(data))

        connection.commit()
        print('successfully imported')
    else:
        print("Error while connecting to MySQL")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    extract_kpos()
    transform()
    import_table()
