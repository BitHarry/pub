#/usr/bin/env python3
import os
import sys
from datetime import datetime, timedelta

#### environment variables
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SRC_PATH = os.path.join(BASE_PATH, "src")
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
AZURE_CONTAINER_NAME = "cr-log-arcdaggbak"
AZURE_BLOB_PATH = f"/ghostcache/archive/arcdaggbak"
##############################
#CERT_FILE = os.path.join(BASE_PATH, ".certs", "server.crt")
#CERT_KEY = os.path.join(BASE_PATH, ".certs", "server.key")

sys.path.append(BASE_PATH)
sys.path.append(DEFAULT_SRC_PATH)
from logger import get_logger
LOGGER = get_logger(file=LOG_FILE, debug=DEBUG, quiet=QUIET)


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



