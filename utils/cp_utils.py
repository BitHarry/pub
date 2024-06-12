#!/usr/bin/env python3

import time
from datetime import datetime, timedelta
import requests
import json
import sqlite3
import pandas as pd
import os
import sys
sys.path.append(os.path.curdir)
from config import LOGGER
from  load_env import load_env



USAGE="""
cp_utils.py is a module that provides utility functions to fetch and process Catchpoint test data and meant to be imported in other scripts.
command line arguments:
    test: runs a test function that fetches folder details and test data for a folder, and prints the results.
    help: prints documentation and usage example
    """

INTERVALS ={
    '5m':'4',
	  '10m':'5',
	  '15m':'6',
	  '30m':'7',
	  '1h':'8',
	  '2h':'11',
	  '3h':'12',
	  '4h':'13',
	  '5h':'15',
	  '6h':'16',
	  '1d':'9'
}
METRIC_IDS= ['1','2','30','4','5','201','15','25','26','27','43']
DIMENSION_IDS=['4','7','2','19','1','6']
SUB_SOURCE_IDS=['4']




class TestData:
    """

    TestData obj represents catchpoint test data.
    The object is returned from the `get_data` function and contains the test data in a 2D table format with dimensions and metrics columns, as well as metadata about the test data.
    
    Usage:
    #fetch catchpoint aggregated test data for last 5 hours for test ids 1234, 5678, 91011, and store it in test_data, a TestData object.
    import cp_utils as cp
    api_key = os.environ['CP_API_KEY']
    test_ids = ['1234', '5678', '91011']
    http_status, test_data = cp.get_data(test_ids=test_ids, time_delta=5.0, api_key=api_key)
    if http_status != 200:
        #hgandle error
    else:
        #print p95 summary of the entire test data grouped by test
        test_data.summary()
        
        #print the p50  of data subset where country is Japan grouped by host
        columns = ['host']
        columns.extend(test_data.metrics)
        columns = ",".join(columns)
        sql=f"SELECT {columns} FROM data WHERE country='Japan'"
        test_data.summary(sql=sql, stat='p50', group_by='host')


    Methods
    -------
       
    query(self, sql:str, conn:sqlite3.Connection=None) -> pd.DataFrame:
        Executes an SQL query on the test data and returns the result as a pandas DataFrame.
    
    summary(self, sql=None, stat:str='p95', group_by='test') -> pd.DataFrame:
        Returns a summary of the test data as a pandas dataframe, grouped by the provided dimension and calculated with the provided statistic. 
        If an SQL query is provided, it is used to filter the data before calculating the summary.

    data_frame(self) -> pd.DataFrame:
        Converts the test data to a pandas DataFrame and returns it.

    to_html(self) -> str:
        Converts the test data to an HTML table and returns it as a string.

    to_csv(self) -> str:
        Converts the test data to a CSV string and returns it.

    to_csv(self, file_path) -> bool:
        Saves the test data as a CSV file at the provided file path. Returns `True` if successful, `False` otherwise.

    data(self) -> dict:
        Converts the test data to a dictionary and returns it.

   
    """
        

    def __init__(self, test_data:dict):
        """ __init__(self, test_data:dict):  init from a dictionary of catchpoint test data."""
        self.columns = test_data['columns']
        self.rows = test_data['rows']
        self.dimensions = test_data['dimension_names']
        self.metrics = test_data['metric_names']
        self.excluded_rows = test_data['excluded_rows']
        self.test_ids = test_data['test_ids']
        self.start_time = test_data['start_time']
        self.end_time = test_data['end_time']
        self.interval = test_data['interval']
        self.metric_ids = test_data['metric_ids']
        self.dimension_ids = test_data['dimension_ids']
        self.sub_source_ids = test_data['sub_source_ids']
      


    def query(self, sql:str, conn:sqlite3.Connection=None) -> pd.DataFrame:
        """query(self, sql:str, conn:sqlite3.Connection=None) -> pd.DataFrame:"""
        return sql_query(
                    sql, 
                    {'rows': self.rows, 'columns': self.columns}, 
                    conn
            )

    def data_frame(self) -> pd.DataFrame:
        """data_frame(self) -> pd.DataFrame: Converts the test data to a pandas DataFrame and returns it."""
        return pd.DataFrame(self.rows, columns=self.columns)
    
    def to_html(self) -> str:
        """to_html(self): Converts the test data to an HTML table and returns it as a string."""
        return self.data_frame().to_html()
    
    def to_csv(self) -> str:
        """to_csv(self): Converts the test data to a CSV string and returns it."""
        return self.data_frame().to_csv(index=False)
    
    def to_csv(self, file_path) -> bool:
        """to_csv(self, file_path): Saves the test data as a CSV file at the provided file path. Returns `True` if successful, `False` otherwise."""
        try:
            self.data_frame().to_csv(file_path, index=False)
            return True
        except Exception as e:
            LOGGER.error(e)
            return False
    
    def data(self) -> dict:
        """data(self): Converts the test data to a dictionary and returns it."""
        result = {}
        for i, row in enumerate(self.rows):
            result[i] = dict(zip(self.columns, row))
        return result

    def summary(self, sql=None, stat:str='p95', group_by='test') -> pd.DataFrame:
        """summary(self, sql=None, stat:str='p95', group_by='test'): Returns a summary of the test data, grouped by the provided dimension and calculated with the provided statistic. If an SQL query is provided, it is used to filter the data before calculating the summary."""
        if group_by not in self.dimensions:
            raise ValueError(f"No such dimension {group_by}")
        
        if sql is not None:
            sub_df = self.query(sql)
        else:
            columns = [group_by] + self.metrics
            sql = f"SELECT {','.join(columns)} from data"
            sub_df = self.query(sql)

        if isinstance(sub_df, pd.DataFrame):
            return get_summary(sub_df, stat=stat, group_by=group_by )
        else:
            LOGGER.error(sub_df)
            return sub_df

    def __str__(self):
        return str(self.data())



####################################################################
    
# call api and return a dict that has a list() of all test_ids in the folder and their metadata
def _fetch_tests_details(folder_id:str, test_type='all', api_key:str=None, logger=LOGGER):
    logger.debug('')
    
    if test_type not in ('web', 'transaction', 'all'):
        logger.error(f"test type {test_type}")
        raise ValueError(f"Unsupported test type {test_type}")

    if api_key is None:  
        load_env() 
        api_key = os.environ['CP_API_KEY']
        if api_key is None:
            logger.error("API key not provided")
            raise ValueError("API key not provided")
        
    params = {

        'parentFolderIds':folder_id,
        'statusId':0,
        'pageNumber':1,
        'pageSize':100,
        'includeAdvanceSettings':True,
        'includeRequest':True,
        'includeInsight':True,
        'includeTargeting':True,
        'includeAlerts':False,
        'showInheritedProperties':True
    }
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer {}'.format(api_key)
    }
    tests_end_point = "https://io.catchpoint.com/api/v2/tests/"
    response = requests.get(tests_end_point, headers=headers, params=params)
    logger.debug(f"Request: {response.request.url}")
    if response.status_code != 200:
        logger.error(f"Error: {response.status_code}")
        logger.error(response.text)
        return (response.status_code, response.text)

    tests_json = response.json()
    tests = {}
    for test in  tests_json['data']['tests']:
        if(test['testType']['name'] == "Transaction"):
            req_data = test['testRequestData']['requestData']
        elif(test['testType']['name'] == "Web"):
            req_data = test['url']
        else:
            req_data='unknown'
        if test_type == 'all' or test['testType']['name'].lower() == test_type:
            tests[str(test['id'])] =  {
                'name': test['name'],
                'requestData' : req_data,
                'type' : test['testType']['name'],
                'monitor': f"{test['monitor']['name']}"
    }
    return (response.status_code, tests)

#--alias
def tests(folder_id:int, test_type='all', api_key:str=None, logger=LOGGER):
    return _fetch_tests_details(folder_id=folder_id, test_type=test_type, api_key=api_key, logger=LOGGER)


def _fetch_folder_details(folder_id, api_key:str=None, logger=LOGGER):
    logger.debug('')
    
    if api_key is None:  
        load_env()  
        api_key = os.environ['CP_API_KEY']
    if api_key is None:
        logger.error("API key not provided")
        raise ValueError("API key not provided")
    
    folder_end_point = f"https://io.catchpoint.com/api/v2/folders/{folder_id}"
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer {}'.format(api_key)
    }
    params = {
        'showInheritedProperties':'true'
    }
    
    response = requests.get(folder_end_point, headers=headers, params=params)
    logger.debug(f"Request: {response.request.url}")
    if response.status_code != 200:
        logger.error(f"Error: {response.status_code}")
        logger.error(response.text)
        return (response.status_code, response.text)
    
    folder_json = response.json()
    nodes = []
    for node in folder_json['data']['folders'][0]['scheduleSetting']['nodes']:
        nodes.append([node['id'],node['name'],node['networkType']['name']])
    return (
        response.status_code,
        { 
            'id': folder_json['data']['folders'][0]['id'], 
            'name': folder_json['data']['folders'][0]['name'],
            'nodes': nodes,
            'response_data': folder_json,
        }
    )
#-------alias 
def folders(folder_id, api_key:str=None, logger=LOGGER):
    """ 
    _folders(folder_id, api_key:str=None, logger=LOGGER) returns (http_status_code, dict with folder details and metadata.)
    """
    return _fetch_folder_details(folder_id=folder_id, api_key=api_key, logger=LOGGER)


def instant_test(test_id:str, api_key:str=None, logger=LOGGER):
   pass 

######################################################################
#return a dict that has test data in a 2d table format with some metadata  
def _extract_test_data(response_data:dict,
                        inc_outliers:bool=True,
                        outlier_threshold:float=2999.99,
                        inc_errors:bool=True,
                        logger=LOGGER) -> dict:
    logger.debug('')
    response_items = response_data['data']['responseItems'][0]
    row_index = 0
    columns = []
    dimension_names = []
    metric_names = []
    rows = []
    excluded =[]
    mismatches= 0
    outliers_count= 0
    errors_count = 0
    potential_outliers_count = 0
    exc_outliers_str = "(Excluded)" if inc_outliers is False else "(Included)"
    exc_errors_str = "(Excluded)" if inc_errors is False else "(Included)"
    real_item_count = 0
    extracted_item_count = 0
    skipped = 0

    for dimension in response_items['dimensions']:
        dimension_names.append(dimension['name'].lower())
    for metric_name in response_items['metrics']:
        metric_name['name'] = metric_name['name'].lower()
        metric_name['name'] = metric_name['name'].replace(' ','_')
        metric_name['name'] = metric_name['name'].replace('%','pct')
        metric_name['name'] = metric_name['name'].replace('(','')
        metric_name['name'] = metric_name['name'].replace(')','')
        metric_name['name'] = metric_name['name'].replace('#','cnt') 
        metric_names.append(metric_name['name'])
    
    logger.debug(f"Dimension names:{dimension_names}")
    logger.debug(f"Metric names:{metric_names}")
    columns = ['row_id'] + dimension_names + metric_names

    #####  data rows #############################
    for item in response_items['items']:
        dimension_values = []
        dimension_values.append(row_index)
        ###### single row dimension values
        for dimension_value in item['dimensions']:
            dimension_values.append(dimension_value['name'])
        ########single row metric values
        metric_values = [0.0 if value is None else float(value) for value in item['values']]
        ### srcub bad data points
        has_outlier = False
        has_error = False
        has_valid_error = False
        for v in metric_values:
            if v >= outlier_threshold:
                has_outlier = True
                break
            if v >= (outlier_threshold * 0.6):
                potential_outliers_count+=1

        if has_outlier is False:
            for i in  range( 0, len(metric_names)-1):
                if metric_names[i].endswith('availibility') and metric_values[i] < 100.0:
                    #possibly a valid error 
                    has_error = False
                    has_valid_error = True
                if metric_names[i].endswith('failures') and metric_values[i] > 0 and has_valid_error is False:
                    has_error = True

        if has_outlier is False and  has_error is False:
            rows.append(dimension_values+metric_values)
        elif has_outlier is True and inc_outliers is True:
            outliers_count=+1
            rows.append(dimension_values+metric_values)
        elif has_error is True and inc_errors is True:
            errors_count+=1
            rows.append(dimension_values+metric_values)
        else:
            excluded.append(dimension_values+metric_values)
            skipped+=1
            if has_outlier:
                outliers_count+=1
            if has_error:
                errors_count+=1
            
            #logger.debug(f":Excluded {excluded[-1]}...")
            #logger.debug(f"Reason: has_error={has_error}. has_outlier={has_outlier}")

        row_index+=1
        #################### end all rows  ############################
    real_item_count = len(response_items['items'])
    extracted_item_count = len(rows) + skipped
    logger.debug(f"Expected data points in current data set: {real_item_count}")
    logger.debug(f"Extracted data points: {extracted_item_count}")
    logger.debug(f"Outliers found: {exc_outliers_str}: {outliers_count}")
    logger.debug(f"Failures found: {exc_errors_str}: {errors_count}")
    logger.debug(f"Total data points excluded: {skipped}")
    logger.debug(f"Potential outliers(included): {potential_outliers_count}")
    logger.debug(f"Data field mismatch count {mismatches}")
    return {
        'rows':rows,
        'columns':columns,
        'dimension_names':dimension_names,
        'metric_names': metric_names,
        'excluded_rows': excluded
    }
#####################################################################
##### FETCH TEST DATA
# call api and return TestData() object that has a 2d table  with dimensions + metrics columns and metadata   
def _fetch_test_data(test_id:str=None,
                    test_ids:list=None,
                    folder_id:str=None,
                    test_type='all',
                    time_delta:float=1.0,
                    start_time:str=None,
                    end_time:str=None,
                    interval:str=INTERVALS['15m'],
                    metric_ids:list=METRIC_IDS,
                    dimension_ids:list=DIMENSION_IDS,
                    sub_source_ids:list=SUB_SOURCE_IDS,
                    api_key:str=None,
                    logger=LOGGER) -> TestData:
    logger.debug('')


    if api_key is None:
        load_env() 
        api_key = os.environ['CP_API_KEY']
    if api_key is None:
        logger.error("API key not provided")
        raise ValueError("API key not provided")
   
    if(test_id is None and test_ids is None and folder_id is None):
        logger.error("input error, throwing exception")
        raise ValueError("test_id[s] or folder_id is a required argument")

    if (test_id is None and test_ids is None):
        logger.debug(f'fetching test info in folder {folder_id}')
        resp_code, tests_details = _fetch_tests_details(folder_id, test_type=test_type,  api_key=api_key)
        if resp_code != 200:
            logger.error(f" {resp_code} response code")
            logger.error(f"{tests_details}")
            return resp_code, tests_details
        elif test_id is None:
           test_ids = tests_details.keys()
        else:
            test_ids.append(test_id)

    if start_time is None:
        start_time = (datetime.utcnow()-timedelta(hours=time_delta)).strftime('%Y-%m-%dT%H:%M:%S')
        end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    else:
        start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')

    num_of_tests = len(test_ids)
    logger.debug(test_ids)
    if num_of_tests < 1:
        logger.error("No test ids found ")
        return 0, "No Test ids Found"

    params = {
        'testIds': ",".join(test_ids),
        'startTime':start_time,
        'endTime':end_time,
        'interval':interval,
        'metricIds': ",".join(metric_ids),
        'dimensionIds':",".join(dimension_ids),
        'subSourceIds':",".join(sub_source_ids)
    }
    logger.debug(f"test ids to fetch: {num_of_tests}")
    for key, value in params.items():
        logger.debug(f"Param: {key}={value}")

    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    test_data_end_point = "https://io.catchpoint.com/api/v2/tests/explorer/aggregated"
    response = requests.get(test_data_end_point, headers=headers, params=params)
    if response.status_code != 200:
        logger.error(f"Req {response.url} error {response.status_code}")
        logger.debug(f"Response: {response.text}")
        return(response.status_code, response.text)
    
    response_data = json.loads(response.text)
    test_data = _extract_test_data(response_data, logger=logger)
    test_data['test_ids'] = test_ids
    test_data['start_time'] = start_time
    test_data['end_time'] = end_time
    test_data['interval'] = interval
    test_data['metric_ids'] = metric_ids
    test_data['dimension_ids'] = dimension_ids
    test_data['sub_source_ids'] = sub_source_ids
   
    return (response.status_code, TestData(test_data))

#---------
def get_data(test_id:str=None,
            test_ids:list=None,
            folder_id:str=None,
            test_type='all',
            start_time:str=None,
            time_delta:float=1.0,
            end_time:str=None,
            interval:str=INTERVALS['15m'],
            metric_ids:list=METRIC_IDS,
            dimension_ids:list=DIMENSION_IDS,
            sub_source_ids:list=SUB_SOURCE_IDS,
            api_key:str=None,
            logger=LOGGER
             ) -> TestData:
    """
    get_data(test_id:str=None,
            test_ids:list=None,
            folder_id:str=None,
            test_type='all',
            start_time:str=None,
            time_delta:float=1.0,
            end_time:str=None,
            interval:str=INTERVALS['15m'],
            metric_ids:list=METRIC_IDS,
            dimension_ids:list=DIMENSION_IDS,
            sub_source_ids:list=SUB_SOURCE_IDS,
            api_key:str=None,
            logger=LOGGER
             ) -> TestData:
    """
    
    
    return _fetch_test_data(
            test_id=test_id,
            test_ids=test_ids,
            folder_id=folder_id,
            test_type=test_type,
            start_time=start_time,
            end_time=end_time,
            time_delta=time_delta,
            interval=interval,
            metric_ids=metric_ids,
            dimension_ids=dimension_ids,
            sub_source_ids=sub_source_ids,
            api_key=api_key,
            logger=logger
    
    )

##########################################################################    


def sql_query(sql:str, test_data:dict, conn:sqlite3.Connection=None, logger=LOGGER) -> pd.DataFrame:
    """sql_query(sql:str, test_data:dict, conn:sqlite3.Connection=None, logger=LOGGER) -> pd.DataFrame:"""
    logger.debug('')
    db_in_memory = False
    if conn is None:
        conn =  sqlite3.connect(':memory:')
        db_in_memory = True

    try:
        assert test_data != {}
        df = pd.DataFrame(test_data['rows'], columns=test_data['columns'])
        df.to_sql('data', conn, index=False, if_exists='replace')
        sub_df = pd.read_sql_query(sql, conn)
        if(sub_df.empty):
            sub_df = "Query did not return any rows"
    except sqlite3.Error as e:
        sub_df = f"sqlite3 error {e}"
    except NameError as e:
        sub_df = "No data in memory, re-run cell one?"
    except AssertionError as e:
        sub_df = "No data in memory, re-run cell one?"
    except Exception as e:
        sub_df = (f"unexpected error {e}")
    finally:
        if db_in_memory:
            conn.close()
        df = None
    
    return sub_df

def get_summary(sub_df:pd.DataFrame, stat:str='p95', group_by='host', logger=LOGGER):
    """get_summary(sub_df:pd.DataFrame, stat:str='p95', group_by='host', logger=LOGGER)"""
    logger.debug('')
    if stat == "p95":
        return sub_df.groupby(group_by).quantile(0.95)
    if stat == "p75":
        return sub_df.groupby(group_by).quantile(0.75)
    if stat == "p50":
        return sub_df.groupby(group_by).quantile(0.50)
    if stat == "mean":
        return sub_df.groupby(group_by).mean()
    if stat == "median":
        return sub_df.groupby(group_by).median()
    raise ValueError(f"Unsupported stat {stat}")


################################## testies 



def _test(folder_id=73339,logger=LOGGER, api_key = os.environ.get('CP_API_KEY')):
    """test(folder_id=73339,logger=LOGGER, api_key = os.environ.get('CP_API_KEY')):"""

    if api_key is None:
        load_env() 
        api_key = os.environ['CP_API_KEY']
    if api_key is None:
        logger.error("API key not provided")
        raise ValueError("API key not provided")

    #### FOLDER TEST #####################33

    logger.info(f"----------------------------Folder ({folder_id})  details---------------------------")
    resp_code, folder_data = _fetch_folder_details(folder_id, api_key=api_key)
    if resp_code != 200:
        logger.error(f"Failed to fetch folder data {resp_code} {folder_data}")
    else:
        logger.info(folder_data['name'])
        logger.info(folder_data['nodes'][:1])
    time.sleep(1)
    
    #### TEST_DETAILS TEST

    test_types = ['web','transaction']

    for test_type in test_types:

        logger.info(f"Test Data for ({folder_id}), {test_type} test type")
        resp_code, test_data  = _fetch_test_data(folder_id=folder_id, test_type=test_type, api_key=api_key)
        logger.info(f"Response code: {resp_code}")
        logger.info(f"Test data: {test_data.__doc__}")
        if resp_code != 200:
            logger.error(f"Failed to fetch test data {resp_code} {test_data}")
        else:
            logger.info(f'dimension names: {test_data.dimensions}')
            logger.info(f'metric names: {test_data.metrics}')
            metrics = ",".join(test_data.metrics)
            sql = f'SELECT test, {metrics}  FROM data where country="Japan" limit 4'
            logger.info(sql)
            df = test_data.query(sql)
            logger.info(df)
            logger.info("testing summary")
            summary = test_data.summary(sql=sql, stat='p50', group_by='test')
            logger.info(summary)
            logger.info('done')
        
        time.sleep(1)

    logger.info('done')


############ extract end #####

if __name__ == "__main__":

    print(sys.path)

    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)
    elif sys.argv[1] == "test":
       _test()
    elif sys.argv[1] == "help":
        print(TestData.__doc__)
    else:
        print(USAGE)
        sys.exit(1)
