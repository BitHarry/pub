#!/usr/bin/env python3
import time
import os

from numpy import sort
from config import LOGGER
from typing import List



def acquire_lock(lock_file:str, max_wait=30, logger=LOGGER):
    time_waited = 0
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

def release_lock(lock_file:str, logger=LOGGER):
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
            logger.debug(f"Removed lock file {lock_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to remove lock file {lock_file}: {e}")
            return False


def chunk_by_char(text: str, size:int=1000) -> List[str]:
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



def chunk_file(file:str, by_char=False, size:int=1000,  output_dir:str="", logger=LOGGER):
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



def get_file_names(directory, ext:str=None, sort_by:str=None, logger=LOGGER)->list[str]:

   
    logger.debug(directory)
    if os.path.isdir(directory) is False:
        raise ValueError(f"{directory} is not a directory")
    
    files = []
    if ext is not None:
        files = [f for f in os.listdir(directory) if f.endswith(ext)]
    else:
        files = list(os.listdir(directory))
    
    if len(files) == 0:
        logger.error(f"No files found in {directory}")
        return None
   
    if sort_by == "index":
       sorted_files = {}
       unsorted_files = []
    for file in files:
        logger.debug(f"File: {file}")
        if file.rfind("_") == -1:
            logger.warning(f"File {file} does not have a valid index,appending to the end of the list")
            unsorted_files.append(file)
            continue
        base = os.path.splitext(file)[0]
        index = base[base.rfind("_")+1:]
        if not index.isdigit():
            logger.warning(f"File {file} does not have a valid index, appending to the end of the list")
            unsorted_files.append(file)
            continue
        sorted_files[int(index)] = file
    sorted_files = dict(sorted(sorted_files.items()))
    
    files = list(sorted_files.values())
    if unsorted_files:
        files += unsorted_files
    return files
   



def pdf_to_text(file_path, logger=LOGGER):
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

def word_to_txt(word_file_name:str, logger=LOGGER):
    import docx2txt
    
    try:
        text = docx2txt.process(word_file_name)
        assert text is not None
        return text
    except Exception as e:
        logger.error(f"Error extracting text from {word_file_name}: {e}")
        return None
    

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
