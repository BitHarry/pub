
import pandas as pd
import os
import sys
from clickhouse_driver import Client
from config import LOGGER as logger,CL_TABLE_DEFINITION, CL_DATATYPES_DICT, CL_TO_DFTYPES_DICT, CL_DB_NAME, CL_DB_PORT, CL_DB_HOST, CL_DB_USER, X_AKA_INFO, X_ES_INFO, AKAMAI_REQUEST_BC
from misc import load_env

def get_client( host=CL_DB_HOST, port=CL_DB_PORT, user=CL_DB_USER, password=None, database=CL_DB_NAME)->Client:
 
    if password is None:
        load_env()
        password= os.environ['CL_PASSWORD']
        if password is None:
            logger.warning("password is unknown, trying with no password")
    
    return Client(host=host, port=port, user=user, password=password, database=database)



def get_dataframe(query:str, table:str, host=CL_DB_HOST, port=CL_DB_PORT, user=CL_DB_USER, password=None, database=CL_DB_NAME):

    client = get_client(host=host, port=port, user=user, password=password, database=database)
    data = client.execute(query)
    column_names = [desc[0] for desc in client.execute(f"DESCRIBE TABLE {table}")]



        
def cast_to_tbl_dtype(row:dict, conversion_dict:dict=CL_DATATYPES_DICT):
    
    if 'req_id'  in row.keys():
        try:
            row['req_id'] = int(row['req_id'])
        except Exception as e:
            row['req_id'] = 0    

    logger.debug(f"conversion dict: {conversion_dict}")

    for df_column_name in row.keys():
        if df_column_name not in  conversion_dict.keys():
            logger.error(f"column {df_column_name} not in {row.keys()}")
            raise ValueError(f"tbl columns {df_column_name} does not match dataframe columns  {row.keys()}")

        if pd.isna(row[df_column_name]) or row[df_column_name] in ['nan', '<NA>']:
            row[df_column_name] = None
            continue
        #i am insecure
        if row[df_column_name] is None:
            continue


        if conversion_dict[df_column_name] == 'Int32':
            try:
                row[df_column_name]= int(row[df_column_name])
            except Exception as e:
                logger.error(f"failed to convert {df_column_name} : {row[df_column_name]} to Int: {e}") 
                row[df_column_name]= None
        elif conversion_dict[df_column_name] == 'Float32':
            try:
                row[df_column_name]= float(row[df_column_name])
            except Exception as e:
                logger.error(f"failed to convert {df_column_name} : {row[df_column_name]} to Float: {e}")
                row[df_column_name]= None
        elif conversion_dict[df_column_name] == 'String':
            try:
                row[df_column_name]= str(row[df_column_name])
            except Exception as e:
                logger.error(f"failed to convert {df_column_name} : {row[df_column_name]} to String: {e}")
                row[df_column_name]= None
        elif conversion_dict[df_column_name] == 'DateTime':
            try:
                row[df_column_name]= pd.to_datetime(row[df_column_name])
            except  Exception as e:
                logger.error(f"failed to convert {df_column_name} : {row[df_column_name]} to datetime: {e}")
                row[df_column_name]= None
        elif conversion_dict[df_column_name] in ('IPv4', 'IPv6'):
            try:
                row[df_column_name]= str(row[df_column_name])
            except Exception as e:
                logger.error(f"failed to convert {df_column_name} : {row[df_column_name]} to IPv4: {e}")
                row[df_column_name]= None
                
        else:
            logger.debug(f"unknown datatype {conversion_dict[df_column_name]}")
            logger.debug(f"conversion dict: {conversion_dict}")
            try:
                row[df_column_name]= str(row[df_column_name])
            except Exception as e:
                logger.error(f"failed to convert {df_column_name} : {row[df_column_name]} to unknown data tye: {e}")
                raise ValueError(f"unknown datatype {conversion_dict[df_column_name]} {e}")

    return row


'''
def create_table(tbl_def_dict:dict={}, client:Client=get_client()):
    logger.debug('')
     
    tble_columns = "" 
    for col in tbl_def_dict.keys():
        tble_columns += f"{col} {" ,".join(tbl_def_dict[col]}, "



    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({tbl_columns})
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(time)
    ORDER BY (time, request, node)
    """
    x = client.execute(create_table_query) 
    logger.debug(f"execute create table query says :{x}")

'''



def upload_df(dataframe: pd.DataFrame, table_name="test", client=get_client()):
    logger.debug('')
    completed = False
    
    
    tbl_schema = get_tbl_schema(table_name, client)
    tbl_columns = ", ".join(tbl_schema.keys())
    if tbl_schema is None:
        logger.error(f"table {table_name} does not exist")
        return False
    total_rows = dataframe.shape[0]
    logger.debug(f"Total number of rows in dataframe: {total_rows}")
    inserted_rows = 0
    total_fails = 0
    fails = 0
    index = 0
        
    for df_row in dataframe.to_dict(orient='records'):
        index += 1
        #need to scrub data and set correct datatypes cus panadas does its own thing
       
        tbl_row = cast_to_tbl_dtype(df_row, conversion_dict=tbl_schema)   
        tbl_columns = ", ".join(tbl_row.keys())
        row_values =  list(tbl_row.values())
        insert_query = f"INSERT INTO {table_name} ( {tbl_columns} ) VALUES "
        try:
            client.execute(insert_query, [row_values], types_check=True)
            inserted_rows += 1
            fails = 0
        except Exception as e:
            logger.error(f"failed to insert row  {index+1} into {table_name}")
            logger.error(f"Error: {e}")
            logger.debug(f"failed row: {tbl_row.values()}")
            logger.debug(f"insert query: {insert_query}")

            fails += 1
            total_fails += 1
            if fails > 10:
                logger.error("too many failed inserts in a row, exiting")
                completed = False
                break
        
    if inserted_rows != total_rows:
        if inserted_rows > 0:
            logger.warning(f" Not all rows were inserted into {table_name}. Total rows: {total_rows}, Inserted rows: {inserted_rows}")
            completed = True
        else:
            logger.error(f" No rows were inserted into {table_name}")
            completed = False

    logger.debug(f"Total inserted  rows {inserted_rows}  for {table_name}")
    logger.debug(f"Total insert failures {total_fails} for {table_name}")
    return completed




def get_tbl_schema(table_name:str, client:Client=get_client()) -> dict: 

    tbl_schema = {}
    try:
        existing_columns = client.execute(f"DESCRIBE TABLE {table_name}")
        assert existing_columns
        assert existing_columns != []
        for col in existing_columns:
            tbl_schema[col[0]] = "".join(col[1:])
        return tbl_schema
    except Exception as e:
        return None

    

def test():
    import json
    import datetime
    import numpy as np

    table_name = "helper_test"
    tbl_columns = """time DateTime, 
        int Nullable(Int32), 
        str Nullable(String),
        float Nullable(Float32),
        bool Nullable(Int8),
        ipv4 Nullable(IPv4),
        ipv6 Nullable(IPv6),
        array Array(String),
        tuple Tuple(String, Int32),
        map Map(String, String)
    """
    test_data  = {
        'time': [
                datetime.datetime(2023, 7, 21, 12, 0, 0), 
                datetime.datetime(2023, 7, 22, 13, 0, 0), 
                datetime.datetime(2023, 7, 23, 14, 0, 0)
            ],
        'int': [np.nan, 42, -100],
        'str': [None, 'example', 'test string'],
        'float': [np.nan, 3.14, 2.718],
        'bool': [1, 0, None],
        'ipv4': [None, '192.168.0.1', '10.0.0.1'],
        'ipv6': [None, '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'fe80::1'],
        'array': [None, ['item1', 'item2'], ['element1', 'element2', 'element3']],
        'tuple': [(None, None), ('tuple string', 1), ('another string', -1)],
        'map': [None, {'key1': 'value1', 'key2': 'value2'}, {'keyA': 'valueA'}] 
    }

    test_df = pd.DataFrame(test_data)
    print(f"Creating {table_name}")
    client = get_client(database="test")

    drop_table_query = f"DROP TABLE IF EXISTS {table_name}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({tbl_columns})
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(time)
    ORDER BY (time)
    """
    res = []
    print(f"Creating table {table_name}")
    res.append(client.execute(drop_table_query))
    res.append(client.execute(create_table_query))
    print(res)
    print(f"Schema for this table  {table_name}")
    tbl_schema = get_tbl_schema(table_name, client)
    print(json.dumps(tbl_schema, indent=4))
    print(f"Uploading data to {table_name}")
    completed = upload_df(test_df, table_name, client)
    print(f"Data upload completed: {completed}")
    res.append(client.execute(f"select * from {table_name}"))


if __name__ == "__main__":
    test()