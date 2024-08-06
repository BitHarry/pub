#/usr/bin/env python3
import os
import sys

#### environment variables
 
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(BASE_PATH, ".env")
EDGERC_FILE = os.path.join(BASE_PATH, ".edgerc")
LOG_FILE = None
SECTION='default' 
CONTRACTID='G-3OBG9M3'
GROUPID='199389'
DEBUG = True
QUIET = False
CL_DB_NAME = "eipb"
CL_DB_USER = "admin"
CL_DB_PORT = "9000"
CL_DB_HOST = "ak-origin.net"

#CERT_FILE = os.path.join(BASE_PATH, ".certs", "server.crt")
#CERT_KEY = os.path.join(BASE_PATH, ".certs", "server.key")

sys.path.append(BASE_PATH)
sys.path.append(os.path.join(BASE_PATH, "src"))
from logger import get_logger
LOGGER = get_logger(LOG_FILE, debug=DEBUG, quiet=QUIET)





#dictionary definitions 



CLOUDS = {
    "tata": ["23.1.99.0/24","23.40.100.0/24","184.31.3.0/24","23.7.244.0/24","104.94.220.0/24","104.94.221.0/24","2600:1480:1000::/40","2600:1480:2000::/40"],
    "ntt": ["23.1.106.0/24","2.18.48.0/24","184.31.10.0/24","2.18.49.0/24","104.94.222.0/24","104.94.223.0/24","2600:1480:3000::/40","2600:1480:4000::/40"],
    "onnet": ["23.1.35.0/24","2.18.52.0/24","184.25.179.0/24","2.18.53.0/24","104.96.178.0/24","104.96.179.0/24","2600:1480:5000::/40","2600:1480:6000::/40"],
    "pccw": ["104.96.176.0/24","104.96.177.0/24","104.109.12.0/24","2.18.50.0/24","184.25.103.0/24","2.18.51.0/24","2600:1480:C000::/40","2600:1480:E000::/40"],
    "overlay": ["104.96.180.0/24","104.96.181.0/24","104.109.10.0/24","2.18.54.0/24","104.109.11.0/24","2.18.55.0/24","2600:1480:8000::/40","2600:1480:A000::/40"],
    "global": ["104.107.211.0/24"],
    "terr-na": ["23.11.32.0/24","23.11.33.0/24","2600:14E1::/46","2600:14E1:4::/46"],
    "terr-sa": ["23.11.34.0/24","23.11.35.0/24","2600:14E1:8::/46","2600:14E1:C::/46"],
    "terr-oc": ["23.11.36.0/24","23.11.37.0/24","2600:14E1:10::/46","2600:14E1:14::/46"],
    "terr-as": ["23.11.38.0/24","23.11.39.0/24","2600:14E1:18::/46","2600:14E1:1C::/46"],
    "terr-eu": ["23.11.40.0/24","23.11.41.0/24","2600:14E1:20::/46","2600:14E1:24::/46"],
    "terr-af": ["23.11.42.0/24","23.11.43.0/24","2600:14E1:28::/46","2600:14E1:2C::/46"]
    }



#breadcrumbs https://collaborate.akamai.com/confluence/pages/viewpage.action?spaceKey=DynamicMedia&title=Breadcrumbs+-+Leaving+a+Trail+for+Customers#BreadcrumbsLeavingaTrailforCustomers-BCtoPlayer/End-User



AKAMAI_REQUEST_BC = {
    "a": ["component_ip", "String"],
    "b": ["req_id", "Int32"],
    "c": ["component_letter", "String"],
    "n": ["component_location", "String"],
    "o": ["component_asn", "Int32"],
}

X_AKA_INFO ={
    "i": ["ak_connected_client_ip", "String"],
    "b": ["ak_eip_forwarder_ip", "String"],
    "g": ["ak_ghost_service_ip", "String"],
    "p": ["ak_client_request_number", "Int32"],
    "r": ["ak_region", "Int32"],
    "t": ["ak_client_rtt", "Int32"],
}

X_ES_INFO = {
    "a": ["client_asnum", "Int32"],
    "l": ["client_city", "String"],
    "c": ["client_country_code", "String"],
    "x": ["client_lat", "Float32"],
    "y": ["client_long", "Float32"],
}



"""    
Cache Parent	c	10000-19999	no
Edge Ghost	g	20000-29999	no
Peer Ghost	p	70000-79999	no
Origin	o	80000-89999	no
Cloud Wrapper	w	90000-99999	yes

    """

COMPONENTS ={
    "c":"CacheParent",
    "g":"EdgeGhost",
    "p":"PeerGhost",
    "o":"Origin",
    "w":"CloudWrapper"
    }



CL_TABLE_DEFINITION = [
            'time DateTime',
            'city String',
            'country String',
            'continent String',
            'host String',
            'networktype String',
            'node String',
            'request String',
            'test String',
            'dns_ms Int32',
            'connect_ms Int32',
            'ssl_ms Int32',
            'wait_ms Int32',
            'ttfb_ms Int32',
           
        ]


for k in X_AKA_INFO:
    CL_TABLE_DEFINITION.append(f"{X_AKA_INFO[k][0].lower()}  {X_AKA_INFO[k][1]}")
for k in X_ES_INFO:
   CL_TABLE_DEFINITION.append(f"{X_ES_INFO[k][0].lower()}  {X_ES_INFO[k][1]}")
for k in AKAMAI_REQUEST_BC:
   CL_TABLE_DEFINITION.append(f"{AKAMAI_REQUEST_BC[k][0].lower()}  {AKAMAI_REQUEST_BC[k][1]}")


CL_TO_DFTYPES_DICT = {}
CL_DATATYPES_DICT = {}
for i in CL_TABLE_DEFINITION:
    k,v = i.split()
    CL_DATATYPES_DICT[k] = v
    CL_TO_DFTYPES_DICT[k] = v.lower()

CL_TO_DFTYPES_DICT['time'] = 'datetime64[ns]'


CP_DATA_TBL = {
    "columns":{
        "time": "DateTime",
        "city": "String",
        "country": "String",
        "continent": "String",
        "host": "String",
        "networktype": "String",
        "node": "String",
        "request": "String",
        "test": "String",
        "dns_ms": "Int32",
        "connect_ms": "Int32",
        "ssl_ms": "Int32",
        "wait_ms": "Int32",
        "ttfb_ms": "Int32",
        "ak_connected_client_ip": "String",
        "ak_eip_forwarder_ip": "String",
        "ak_ghost_service_ip": "String",
        "ak_client_request_number": "Int32",
        "ak_region": "Int32",
        "ak_client_rtt": "Int32",
        "client_asnum": "Int32",
        "client_city": "String",
        "client_country_code": "String",
        "client_lat": "Float32",
        "client_long": "Float32",
        "component_ip": "String",
        "req_id": "Int32",
        "component_letter": "String",
        "component_location": "String",
        "component_asn": "Int32",
    },
    "primary_keys": ["time", "node", "test"]
}


EIPB_TABLES ={}
EIPB_TABLES['cp_data'] = CP_DATA_TBL


           
if __name__ == "__main__":
    
    if len(sys.argv) > 0 and sys.argv[1] == "test":
        l = LOGGER
        for p in sys.path:
            l.info(p)
        import json
        l.info("*************CL TABLE DEF*********************")
        l.info(json.dumps(CL_TABLE_DEFINITION, indent=2))
        l.info("************CL DATATYPES*************")
        l.info(json.dumps(CL_DATATYPES_DICT, indent=2))
        l.info("************CL to DF DATATYPES*************")
        l.info(json.dumps(CL_TO_DFTYPES_DICT, indent=2))
    else:
        print("i am not meant to be run directly")
        sys.exit(1)
