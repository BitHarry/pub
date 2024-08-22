#!/usr/bin/env python3
import os
from datetime import datetime, timedelta
import sys
from azure.storage.blob import ContainerClient, BlobClient, BlobProperties
from tqdm import tqdm
curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_dir)
sys.path.append(os.path.join(curr_dir, 'src'))

from utils import load_env, hdrs_to_dict  
from config import AZURE_CONTAINER_NAME, AZURE_BLOB_PATH,  LOGGER as logger 
#from arcdlogsdb import Blob, CombinedFile, ArcdLogsDB
load_env()
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

#service_client = BlobServiceClient.from_connection_string(conn_str=AZURE_STORAGE_CONNECTION_STRING)

container_client = ContainerClient.from_connection_string(
    conn_str=AZURE_STORAGE_CONNECTION_STRING, 
    container_name=AZURE_CONTAINER_NAME,
    logging_enable=True
    )

def to_timestamp(epoch:int) -> str:
    try:
        date = datetime.fromtimestamp(epoch)
        return date.strftime("%Y%m%d%H")
    except Exception as e:
        logger.debug(f"Failed to convert epoch to timestamp: {e}")
        logger.debug(f"epoch: {epoch} is not a valid unix timestamp")
        sys.exit(1)
  
def get_blob_client(blob_name):
    blob_client = BlobClient.from_connection_string(
        conn_str=AZURE_STORAGE_CONNECTION_STRING, 
        container_name=AZURE_CONTAINER_NAME, 
        blob_name=blob_name,
        logging_enable=True
    )
    if not blob_client.exists():
        logger.debug(f"blob_client: {blob_name} does not exist")
        return None
    return  blob_client

def list_blob_names(epoch_timestamp, window:int=1, metadata_only:bool=True) -> list:
    timestamp = to_timestamp(epoch_timestamp)
    name_starts_with = AZURE_BLOB_PATH + "/" + str(timestamp)
    logger.debug(f"fetching blob names with name_starts_with: {name_starts_with}")
    blobs_list = []

    while(window > 0):
        blobs = container_client.list_blob_names(name_starts_with=name_starts_with, logging_enable=True)
        for blob in blobs:
            blob_file_name = blob.split('/')
            if not  blob_file_name[-1].endswith(".done"):
                #logger.debug(f"Ignoring:{blob}")
                continue
            if metadata_only and not "metadata" in blob:
               # logger.debug(f"Ignoring:{blob}")
                continue
            else:
                blobs_list.append(blob)
        window -= 1
        timestamp = to_timestamp(epoch_timestamp + 3600)
        name_starts_with = AZURE_BLOB_PATH + "/" + str(timestamp)
        logger.debug(f"name_starts_with: {name_starts_with}")
    
    logger.debug(f"fetched {len(blobs_list)} blob names")
    #sorted_blobs_list = sorted(blobs_list, key=lambda x: x.timestamp, reverse=False)
    return blobs_list



def list_blobs(epoch_timestamp, window:int=1, metadata_only:bool=True) -> list:
    timestamp = to_timestamp(epoch_timestamp)
    name_starts_with = AZURE_BLOB_PATH + "/" + str(timestamp)
    #logger.debug(f"fetching blobs with name_starts_with: {name_starts_with}")
    blobs_list = []
    
    while(window > 0):
        blobs = container_client.list_blobs(name_starts_with=name_starts_with, include=['tags'], logging_enable=True)
        for blob in blobs:
            blob_file_name = blob.name.split('/')
      
         

            if not  blob_file_name[-1].endswith(".done"):
                #logger.debug(f"Ignoring:{blob}")
                continue
            if metadata_only and not "metadata" in blob.name:
                #logger.debug(f"Ignoring:{blob}")
                continue

            else:
                blobs_list.append(blob)
        window -= 1
        timestamp = to_timestamp(epoch_timestamp + 3600)
        name_starts_with = AZURE_BLOB_PATH + "/" + str(timestamp)
     #   logger.debug(f"name_starts_with: {name_starts_with}")
    
    #logger.debug(f"fetched {len(blobs_list)} blobs  for timestamp: {timestamp}  window: {window}")
    #sorted_blobs_list = sorted(blobs_list, key=lambda x: x.timestamp, reverse=False)
    return blobs_list





def fetch_metadata(blob_name):

    if "metadata" not in blob_name:
        logger.error(f"blob_name: {blob_name} is not a metadata file")
        return None

    blob_client = get_blob_client(blob_name)
    if blob_client:
        blob_data =  blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
        if blob_data:
            return hdrs_to_dict(blob_data.readall())
        else:
            logger.error(f"Failed to get metadata for {blob_name}")
            return None
    else:
        logger.error(f"Failed to get blob client {blob_name}")
        return None



def download_blob(blob_name, output_file_name) -> bool:

    #logger.debug(blob_name)  
    blob_client = get_blob_client(blob_name)
    if blob_client:
        blob_data = blob_client.download_blob()
        if blob_data:
            with open(output_file_name, 'ab') as f:
                f.write(blob_data.readall())
            return True
        else:
            logger.error(f"Failed to download {blob_name}")
            return False
    else:
        logger.error(f"Failed to get blob client {blob_name}")
        return False





def download_logs(window:int=1, 
             delta:int=1, 
             output:str='arcdagg.log', 
             regions:list=[], 
             start_time:int=0,
             list_only:bool=False) -> None:
    
    if start_time == 0:
        now = datetime.now()
        delta = timedelta(hours=delta)
        start_time =  int((now - delta).timestamp())
    
    if regions != []:
        logger.debug(f"Regions: {regions}")
        regions = [str(r) for r in regions]

    blob_list = list_blob_names(start_time, window)
    logger.debug(f"Got {len(blob_list)} potential blobs with start_time:{start_time} window:{window}")
    
    ## get metadata  for available blobs
    blobs_to_download = []
    for blob in tqdm(blob_list, desc="Fething metadata", total=len(blob_list)):
        blob_metadata = fetch_metadata(blob)
        if blob_metadata:
            blob_file_name = blob.replace("metadata", "0")
            if regions != []:
                if blob_metadata["X-Akamai-Region"] in regions:
                    #logger.debug(json.dumps(blob_metadata, indent=2))
                    #logger.debug(f"Found blob to download for region: {blob_metadata['X-Akamai-Region']}")
                    #logger.debug(f"BlobFile {blob_file_name}")
                    blobs_to_download.append(blob_file_name)
            else:
                blobs_to_download.append(blob_file_name)
                
        else:
            logger.error(f"Failed to fetch metadata for {blob}")
    
    blob_count = len(blobs_to_download)
    blobs_downloaded = 0
    blobs_to_download = sorted(blobs_to_download)
    logger.debug(f"{blob_count} blobs available for download")
    
    if list_only:
        blob_names_str = "\n".join(blobs_to_download)
        output = f"{output}.blobs.txt" if not output.endswith(".txt") else output
        with open(output, 'w') as f:
            f.write(blob_names_str)
        logger.info(f"{blob_count} blob names saved to {output}")
        return        
    
    output = f"{output}.zstd" if not output.endswith(".zstd") else output
    for blob in tqdm(blobs_to_download, desc="Downloading blobs", total=blob_count):
        ok = download_blob(blob, output)
        if ok:
            blobs_downloaded += 1
            #logger.debug(f"Downloaded {blob}")
        else:
            logger.error(f"Failed to download {blob}")
        
    logger.info(f"Downloaded {blobs_downloaded} blobs out of {blob_count} to {output}")
    return output



     



"""
 Content-type: octet/binary
Date: Sat, 10 Aug 2024 18:42:20 +0000
From: daemon
MIME-Version: 1.0
Message-Id: <2s0ix-zvlcs-rmkwa-73if9-44772-404@23.56.2.75>
Subject: 23.56.2.75 /usr/local/akamai/logs/arcd.aggregatedconns.log.1.gz
To: logs.ddc.akadns.net
X-Akamai-Ddc-Odfn: arcd.aggregatedconns.log
X-Akamai-Encoding: off
X-Akamai-Header-Version: 2.0
X-Akamai-Host: logs.ddc.akadns.net
X-Akamai-IPv6: 2600:1403:8400::1738:24b
X-Akamai-Local-Path: /usr/local/akamai/logs/arcd.aggregatedconns.log.old/arcd.aggregatedconns.log.2s0ix-zvlcs-rmkwa-73if9-44772.404.1723311583.1723315294.gz
X-Akamai-Log-Size: 6304241
X-Akamai-Network: brave
X-Akamai-Region: 44079
X-Akamai-Rotation-ID: 202408100018
X-Akamai-Send-Epoch: 1723315340
X-Akamai-Sequence: arcd.aggregatedconns.log 2s0ix-zvlcs-rmkwa-73if9-44772 404
X-Akamai-Src-Ip: 23.56.2.75

"""