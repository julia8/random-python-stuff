import datetime
import pandas as pd
import numpy as np
import requests
import mysql.connector
from contextlib import closing


def convert_to_values_list(x):
    val = np.array2string(x, separator=",")
    val = val.replace('[', '(').replace("]", ')')
    print(val)
    return val

def get_db_connection():
    return mysql.connector.connect(user='appuser', password='appuser', host='127.0.0.1', database='testdb')


def replace_in_db(tbl_name, dataframe, del_criteria):
    with closing(get_db_connection()) as conn:
        with conn.cursor() as cursor:
            cursor.execute("delete from {}".format(tbl_name)) # delete
            cursor.execute("insert into {} (for_date, station_id, name) values ('2017-12-01', 1, 'abc')")



def build_test_data_frame():
    content = requests.get("https://gbfs.citibikenyc.com/gbfs/en/station_information.json").json()
    data = content["data"]["stations"]
    print("Stations: {}".format(len(data)))

    df = pd.DataFrame(data)
    df.insert(0, 'for_date', datetime.date.today().strftime('%Y-%m-%d'))
    del df['rental_methods']
    df.fillna(0)
    return df


replace_in_db('stations', '', '')

df = build_test_data_frame()

cols = ",".join(list(df))
print(cols)
to_insert = [convert_to_values_list(x) for x in df.values]
#tuples = [print_to_console(x) for x in df.values]

sql = "insert into {} ({}) values {}".format("stations", cols, ",".join(to_insert))
print(sql)
