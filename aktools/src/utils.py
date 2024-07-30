#!/usr/bin/env python3
import os
from akvars import LOGGER as logger



def to_subnet(value)->str:
    """verifies if the input is a valid IP address or subnet and returns the subnet."""
    import ipaddress
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


def get_srv_mon_ips(country:str=None, continent:str=None)->list:
    """return a list of GTM server monitor ip for given country or continent"""
    
    import query2_utils as q2
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
    logger.debug(f"{command}]\n{result.stdout}")

   
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




def get_cloud_name(ip_or_subnet)->str:
    from akvars import CLOUDS
    import ipaddress
    try:
        subnet = to_subnet(ip_or_subnet)
    except ValueError: 
        return "NULL"
    
    for cloud in CLOUDS:
        for cloud_subnet in CLOUDS[cloud]:
            if ipaddress.ip_network(subnet).overlaps(ipaddress.ip_network(cloud_subnet)):
                return cloud
    return "NULL"










def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


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
