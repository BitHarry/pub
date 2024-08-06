#!/usr/bin/env python3

import sys
import os
import importlib.util

USAGE = """
curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(curr_dir,'pie_tools'))

from pietools import catchpoint_helper as cp, clickhouse_helper as cl, LOGGER as logger


#getting data from catchpoint api
status_code, data =  status_code, data = cp.get_data(test_ids=test_ids, time_delta =0.50, sub_source_ids=['4'], dimension_ids=dimension_ids, metric_ids=metric_ids, data_type='raw')

#uploading data to clickhouse
df = data.data_frame()
completed = cl.upload_cp_data(df)

"""

DEFAULT_SRC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")

def import_src(src_file:str, src_path:str=None):
    if src_file in sys.modules:
        return sys.modules[src_file]
    
    if not src_path:
        src_path = DEFAULT_SRC_PATH
        if not os.path.isdir(src_path):
            raise(f"either '{src_path}'  or '{__file__}' is in wrong location")
    else:
        if not os.path.isdir(src_path):
            raise(f"'{src_path}' is not a valid directory")

    spec = importlib.util.spec_from_file_location(src_file, os.path.join(src_path,src_file))
    module = importlib.util.module_from_spec(spec)
    sys.modules[src_file] = module
    spec.loader.exec_module(module)
    return module

if os.path.isdir(DEFAULT_SRC_PATH):
    sys.path.append(DEFAULT_SRC_PATH)
    src_files = [f for f in os.listdir(DEFAULT_SRC_PATH) if f.endswith('.py')]
    for src_file in src_files:
         import_src(src_file)
else:
    print(f"ERROR: There is no {DEFAULT_SRC_PATH}  here")
    sys.exit(1)

from config import LOGGER
import catchpoint_helper
import clickhouse_helper
import misc
import query2_helper


if __name__ == "__main__": 

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        import_src('config.py')
        from config import BASE_PATH
        LOGGER.info(BASE_PATH)
  
    else:
        print("i am not meant to be run directly")
            

  

    
      
     