#!/usr/bin/env python3
import os
import sys
from config import ENV_FILE

def load_env(env_file:str=ENV_FILE):
    """
    Load environment variables from a file
    """
    loaded_vars = []
    if not os.path.exists(env_file):
        raise Exception(f"cannot find {env_file}")           
    
    try:
        with open(env_file, 'r') as file:
            for line in file:
                if line.strip(): 
                    key, value = line.strip().split('=', 1)
                    assert key
                    assert value
                    os.environ[key] = value
                    loaded_vars.append(key)
    except Exception as e:
        raise Exception(f"failed to load vars from {env_file} : {e}")
    
    return loaded_vars

def _test():
    from config import LOGGER
    LOGGER.debug(ENV_FILE)
    LOGGER.info(load_env())


      

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("This module is not meant to be imported.")
        sys.exit(1)
    
    if sys.argv[1] == 'test':
        _test()
    
    