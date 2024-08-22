
import pandas as pd
import os
import sys
from clickhouse_driver import Client
from config import LOGGER as logger, CL_DB_NAME, CL_DB_PORT, CL_DB_HOST, CL_DB_USER
from utils import load_env
import decimal
import ipaddress
import uuid
import json
from tqdm import tqdm



def get_client( host=CL_DB_HOST, port=CL_DB_PORT, user=CL_DB_USER, password=None, database=CL_DB_NAME)->Client:
 
    if password is None:
        load_env()
        password= os.environ['CL_PASSWORD']
        if password is None:
            logger.warning("password is unknown, trying with no password")
    
    return Client(host=host, port=port, user=user, password=password, database=database)



def cast_to_tbl_dtype(row, conversion_dict):
    
    def convert_value(value, dtype):
   
        if 'Nullable' in dtype:
            for n in ['nan', '<NA>', 'NaN']:
                if n in str(value):
                    return None 
            dtype = dtype.replace('Nullable(', '').replace(')', '')       
        
        try:
            if dtype.startswith('Int') or dtype.startswith('UInt'):
                return int(value) if value is not None else None
            
            if dtype.startswith('Float'):
                return float(value) if value is not None else None
            
            if dtype == 'String' or dtype.startswith('FixedString'):
                return str(value) if value is not None else None
            
            if dtype == 'Date':
                return pd.to_datetime(value).date() if value is not None else None
            
            if dtype.startswith('DateTime'):
                return pd.to_datetime(value) if value is not None else None
            
            if dtype.startswith('Decimal'):
                return decimal.Decimal(value) if value is not None else None
            
            if dtype == 'UUID':
                return uuid.UUID(value) if value is not None else None
            
            if dtype == 'IPv4':
                return ipaddress.IPv4Address(value) if value is not None else None
            
            if dtype == 'IPv6':
                return ipaddress.IPv6Address(value) if value is not None else None
            
            if dtype.startswith('Array'):
                return value if value is not None else []
            
            if dtype.startswith('Tuple'):
                return tuple(value) if value is not None else None
            
            if dtype.startswith('Map'):
                return dict(value) if value is not None else {}
            
            if dtype == 'JSON':
                return json.loads(value) if value is not None else None
        except Exception as e:
            logger.debug(f"Exception:Failed to convert value {value} to type {dtype}: {e}")
            return None
        
        return value  # Fallback for any other types
    
    converted_row = {key: convert_value(value, conversion_dict[key]) for key, value in row.items()}
    return converted_row



def upload_df(dataframe: pd.DataFrame, table_name="test", client=get_client()):
    logger.debug(f"Uploading dataframe to {table_name}")    
    tbl_schema = get_tbl_schema(table_name, client)
    tbl_columns = ", ".join(tbl_schema.keys())
    if tbl_schema is None:
        logger.error(f"table {table_name} does not exist")
        return None
    total_rows = dataframe.shape[0]
    logger.debug(f"Total number of rows in dataframe: {total_rows}")
    failed_rows = []
    inserted_row_count = 0
    fails = 0
    index = 0
        
    for df_row in tqdm(dataframe.to_dict(orient='records'), desc="Loading data", total=total_rows):
        #need to scrub data and set correct datatypes cus panadas does its own thing
       
        tbl_row = cast_to_tbl_dtype(df_row, conversion_dict=tbl_schema)   
        tbl_columns = ", ".join(tbl_row.keys())
        row_values =  list(tbl_row.values())
        insert_query = f"INSERT INTO {table_name} ( {tbl_columns} ) VALUES "
        try:
            client.execute(insert_query, [row_values], types_check=True)
            inserted_row_count += 1
            fails = 0
        except Exception as e:
            logger.error(f"Insert Error at index {index}: {e}")
            logger.debug(f"columns: {tbl_columns}")
            logger.debug(f"failed original values: {dataframe.iloc[index]}")
            logger.debug(f"failed prepped values: {row_values}")
            failed_rows.append(index)
            #logger.debug(f"insert query: {insert_query}")
            fails += 1
            if fails > 10:
                logger.error("too many failed inserts in a row, exiting")
                break
        finally:
            index += 1  

        
    if  failed_rows != []:
        if inserted_row_count > 0:
            logger.warning(f" Not all rows were inserted into {table_name}")
            logger.debug(f"Failed to insert rows at indexes: {failed_rows}")
        else:
            logger.error(f" Failed to insert anything into {table_name}")    
    else:
        logger.debug(f"All {total_rows} rows were inserted into {table_name}")

    return (failed_rows)




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


def update_arcd_machines(client:Client=get_client()):
    logger.debug("Updating arcd_machines table")

    arcd_machines_sql = """SELECT 
                            m.ip as ip, 
                            m.region as region, 
                            m.regionName as region_name, 
                            CASE 
                                WHEN SUBSTR(anycastname, STRLEN(a.anycastname)-1, 2) = '-b' THEN REPLACE(SUBSTR(a.anycastname, 1, STRLEN(anycastname)-2), 'eip-', '')
                                WHEN SUBSTR(anycastname,STRLEN(a.anycastname)-1, 2) = '-a' THEN REPLACE(SUBSTR(a.anycastname, 1, STRLEN(a.anycastname)-2), 'eip-', '')
                                ELSE a.anycastname
                            END AS cloud,             
                            l.city as city, 
                            l.state as state, 
                            l.metro as metro, 
                            l.country as country, 
                            l.continent as continent,  
                            CAST(l.latitude AS FLOAT) / 1000.00 as lat, 
                            CAST(l.longitude AS FLOAT) / 1000.00 as long 
                        FROM 
                            mcm_machines m, 
                            mcm_regionlocation l,
                            routing_region_v2_postinstall a 

                        WHERE 
                            m.region = l.physicalRegion 
                            AND m.network = 'brave'
                            AND m.region = a.region 
                            AND a.anycastname NOT LIKE  '%eip-global%'
                            AND a.anycastname NOT LIKE  '%overlay%'
                        GROUP BY 
                            1 
                        ORDER BY 
                            2,9,4,7
            """

    import query2_helper as q2
    logger.debug("fething data from brave agg...")
    agg = q2.get_agg('brave')
    try:
        df = q2.query(arcd_machines_sql, agg, pyquery=True)
        assert df is not None or not df.empty
    except Exception as e:
        logger.error(f"Failed to get arcd_machines data: {e}")
        return False
    logger.debug("success")
           
    logger.debug('uploading arcd_machines to clickhouse')
    failed_rows = upload_df(df, table_name="arcd_machines", client=client)
    if failed_rows != []:
        logger.debug(f"{len(failed_rows)} failed rows")

    sql = f"SELECT count(*) FROM arcd_machines;"
    res = client.execute(sql)
    logger.debug(f"arcd_machines count: {res}")
    return True



def get_arcd_machines(client=get_client())->dict:
    sql = f"SELECT ip, region, region_name, cloud FROM arcd_machines"
    res = client.execute(sql)
    arcd_machines = {}
    for ip, region, region_name, cloud in res:
        arcd_machines[ip] = {
            "region": region,
            "region_name": region_name,
            "cloud": cloud
        }
    return arcd_machines



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
        array Array(Nullable(String)),
        tuple Tuple(Nullable(String), Nullable(Int32)),
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
    print(res)


if __name__ == "__main__":
    test()