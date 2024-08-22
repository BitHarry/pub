#!/usr/bin/env python3
"""
miscaleanous  re-usable utility functions
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import  ENV_FILE, DEFAULT_SRC_PATH, LOGGER as logger
import time
from datetime import datetime
import math
import ipaddress
import pandas as pd

def haversine(lat1:float, lon1:float, lat2:float, lon2:float)->float:
    if not all([lat1, lon1, lat2, lon2]):
        logger.debug(f"bad value: lat1: {lat1} lon1: {lon1} lat2: {lat2} lon2: {lon2}")
        return None
    
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R = 6371.0
    distance = R * c
    return float(f"{distance:.2f}")



def tbl_to_dataframe(file_path:str)->pd.DataFrame:
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    column_names = lines[0].strip().split(',')
    column_types = lines[1].strip().split(',')
    dtype_mapping = {
        'int': 'int64',
        'string': 'object',
        'float': 'float64',
        'bool': 'bool',
        'ip': 'object',
    }
    dtype_dict = {}
    for col_name, col_type in zip(column_names, column_types):
        base_type = col_type.rstrip('?')
        dtype_dict[col_name] = dtype_mapping.get(base_type, 'object')
    data_frame = pd.read_csv(file_path, skiprows=2, names=column_names, dtype=dtype_dict)
    
    return data_frame


def read_compressed_file(file_path:str)->list:
    """
    Reads a compressed file and returns its contents as a list of lines.
    Args:
        file_path (str): The path to the compressed file.
    Returns:
        list: The contents of the file as a list of lines.
    Raises:
        ValueError: If the compression type is unknown or the file is not a text file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist")

    import zstandard as zstd
    import gzip
    try:
        with open(file_path, 'rb') as compressed_file:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(compressed_file) as reader:
                decompressed_data = reader.read().decode('utf-8')
                lines = decompressed_data.splitlines(keepends=True)
                return lines
    except zstd.ZstdError:
        pass  

    try:
        # Try to decompress using gzip
        with gzip.open(file_path, 'rt', encoding='utf-8') as text_file:
            lines = text_file.readlines()
            return lines
    except (OSError, gzip.BadGzipFile):
        pass  

    try:
        with open(file_path, 'r', encoding='utf-8') as text_file:
            lines = text_file.readlines()
            return lines
    except UnicodeDecodeError:
        raise ValueError(f"Failed to decompress and decode {file_path}")


def import_src(src_file:str, src_path:str=None):
    import importlib.util
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

def to_epoch(timestamp):
 
    logger.debug('')

    if isinstance(timestamp, int):
        if(len(str(timestamp)) == 10 and  not str(timestamp).startswith("20")):
        
            time_diff =  int(timestamp) - int(time.time()) 
            logger.debug(f"Time diff is {time_diff}")
            if(time_diff > 2592000):
                logger.error(f"old timestamp {timestamp} ")
                raise ValueError("Invalid Start Time: Start time is more then 30 days ago")
            if(time_diff >= 86400):
                logger.error(f"too far in futer timestamp {timestamp} ")
                raise ValueError("Invalid Start Time: Start time should not be more then 24h into the future")
            return int(timestamp) 
        
    try:
        timestamp = ''.join(char for char in str(timestamp) if char.isdigit())
        year = int(timestamp[:4])
        month = int(timestamp[4:6])
        day = int(timestamp[6:8])
        hour = int(timestamp[8:10])
        minute = int(timestamp[10:12])
        date = datetime.datetime(year, month, day, hour, minute)
        epoch = int(date.timestamp())
        assert year is not None and len(str(year)) == 4
        assert month is not None and month < 13
        assert day is not None and day < 32
        if(hour is None):
            hour = 1
        if(hour == 0):
            hour = 1
        assert hour < 24 
    except Exception as e:
        logger.error(f"Invalid timestamp {timestamp} ")
        raise ValueError("Invalid Stat Time format: Supporter format is epoch or YYYYMMDDHH, or YYYY-MM-DD-HH (hours is in 24h format 1-24. if no hour is provided, 00 is assumed, minutes)")
    
    return epoch




def ip_to_subnet(value)->str:
    """verifies if the input is a valid IP address or subnet and returns the subnet."""
    try:
        subnet = ipaddress.ip_network(value)
        return str(subnet)
    except ValueError:
        pass

    try:
        ip = ipaddress.ip_address(value)
        if ip.version == 4:
            subnet = ipaddress.ip_network(f"{value}/32")
        else:
            subnet = ipaddress.ip_network(f"{value}/128")
        return str(subnet)
    except ValueError:
        pass

    raise ValueError(f"Invalid IP address or subnet: {value}")

def to_subnet(value:str)->str:
    return ip_to_subnet(value)

def subnet_to_ip(netblock:str)->str: 
    network = ipaddress.ip_network(netblock)
    return str(network[2])

def to_ip(value:str)->str:
    return subnet_to_ip(value)





def _traceroute2(host_or_ip:str)->list:
    
    from scapy.all import traceroute
    res, unans = traceroute(host_or_ip, maxttl=20)
    return res

def _traceroute(host_or_ip:str)->list:
    import subprocess    
    import re
    trace = []
    command = ["traceroute", "-4n", host_or_ip]
    result = subprocess.run(command, capture_output=True, text=True)
    logger.debug(f"{command}\n{result.stdout}")

   
    for line in result.stdout.split("\n"):
        line = line.replace("ms","")
        hop = line.split()
        if hop and hop[0].isdigit():
            ip = re.findall(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', line)
            logger.debug(f"hop: {hop} ip: {ip}")
            if ip == []:
                ip = [None]
            else:
                ip = list(set(ip))

            rtt = re.findall(r'\s(\d+\.\d+)\s', line)
            if rtt == []:
                rtt = [None]


            trace.append({
                "hop": hop[0],
                "ip": ip,
                "rtt": rtt,
            })
    return trace


def hdrs_to_dict(hdr:str) -> dict:
    hdr_dict = {}
    lines = hdr.strip().split('\n')
    for line in lines:
        key, value = line.split(':', 1)
        hdr_dict[key.strip()] = value.strip()
    return hdr_dict




def cast_to_type(value, lower=True):
    if value is None:
        return None
    try:
        value =  int(value)
        logger.debug(f"casted {value} to int")
        return value
    except ValueError:
        pass
    
    try:
        value = float(value)
        logger.debug(f"casted {value} to float")
        return value
    except ValueError:
       pass
    
    if lower:
        return str(value.lower())
    return str(value)

def acquire_lock(lock_file:str, max_wait=3  ):
    time_waited = 0
    import time
    while os.path.exists(lock_file):
        time.sleep(1)
        time_waited+=1
        if(time_waited > max_wait):
            print(f"{lock_file} locked too long, proceeding anyways")
            break
    try:
        open(lock_file, 'w').close()
        logger.debug(f"Created lock file {lock_file}")
    except Exception as e:
        logger.error(f"Failed to create lock file {lock_file}: {e}")
        return False

def release_lock(lock_file:str):
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
            logger.debug(f"Removed lock file {lock_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to remove lock file {lock_file}: {e}")
            return False


def chunk_by_char(text: str, size:int=1000) -> list[str]:
    return [text[i:i + size] for i in range(0, len(text), size)]

def chunk_by_words(text: str, size:int=300) -> list[str]:
    words = text.split()
    chunks = []
    current_chunk = ""
    count = 0
    for word in words:
        if  count + 1 > size:
            chunks.append(current_chunk)
            current_chunk = word
            count = 0
        else:
            if current_chunk:
                current_chunk += " " + word
            else:
                current_chunk = word
            count += 1

    if current_chunk:
        chunks.append(current_chunk)

    return chunks



def chunk_file(file:str, by_char=False, size:int=1000,  output_dir:str=""):
    with open(file, 'r') as f:
        text = f.read()
    if by_char:
        chunks = chunk_by_char(text, size=size)
    else:
        chunks = chunk_by_words(text, size=size)
    for i, chunk in enumerate(chunks):
        output_file = os.path.join(output_dir, f"{file}_{i}.txt")
        with open(output_file, 'w') as f:
            f.write(chunk)
            logger.debug(f"Created chunk file {output_file}")
    logger.debug(f"Created {len(chunks)} chunk files")
    return len(chunks)



def get_file_names(directory, ext=None)->list[str]:
    logger.debug(directory)
    if os.path.isdir(directory) is False:
        raise ValueError(f"{directory} is not a directory")
    
    files = []
    if ext is not None:
        files = [f for f in os.listdir(directory) if f.endswith(ext)]
    else:
        files = list(os.listdir(directory))
    if len(files) > 0:
        return files
    if len(files) == 0:
        raise ValueError(f"No files found in {directory}")



def pdf_to_text(file_path):
    import PyPDF2
    try:
        pdf_file_obj = open(file_path, 'rb')
        pdf_reader = PyPDF2.PdfFileReader(pdf_file_obj)
        text = ''
        for page_num in range(pdf_reader.numPages):
            page_obj = pdf_reader.getPage(page_num)
            text += page_obj.extractText()
        pdf_file_obj.close()
        return text
    except Exception as e:
        logger.error(f"Error extracting text from {file_path}: {e}")
        return None

def word_to_txt(word_file_name:str):
    import docx2txt
    
    try:
        text = docx2txt.process(word_file_name)
        assert text is not None
        return text
    except Exception as e:
        logger.error(f"Error extracting text from {word_file_name}: {e}")
        return None


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
    logger.debug(ENV_FILE)
    logger.info(load_env())


## query2 utils

def get_srv_mon_ips(country:str=None, continent:str=None)->list:
    """return a list of GTM server monitor ip for given country or continent"""
    
    import query2_helper as q2
    if country:
        location = f" AND l.country  = '{country}' "
    elif continent: 
        location = f" AND l.continent  = '{continent}' "
    else:
        location = ""
        
    logger.debug(f"location: {location}")
    query = f"""
        SELECT 
        distinct s.ip as ip
        FROM
            servermonitor s, MCM_regionLocation l, mcm_machines m
        WHERE
            s.ip = m.ip
        AND
            m.region = l.physicalRegion
        {location}
        ORDER BY 1
    """
    return q2.query(query,'map').values.ravel().tolist()




      

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("This module is not meant to be imported.")
        sys.exit(1)
    
    if sys.argv[1] == 'test':
        _test()
    
    