#/usr/bin/env python3
import os
import sys



#### environment variables
 
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(BASE_PATH, ".env")
EDGERC_FILE = os.path.join(BASE_PATH, ".edgerc")
LOG_FILE = os.path.join(BASE_PATH, "main.log")
SECTION='default' 
CONTRACTID='G-3OBG9M3'
GROUPID='199389'
DEBUG = True
QUIET = True

#CERT_FILE = os.path.join(BASE_PATH, ".certs", "server.crt")
#CERT_KEY = os.path.join(BASE_PATH, ".certs", "server.key")

sys.path.append(BASE_PATH)
sys.path.append(os.path.join(BASE_PATH, "src"))
default_vars = False
try:
    import config as c
    ENV_FILE = c.ENV_FILE if hasattr(c, 'ENV_FILE') else ENV_FILE
    EDGERC_FILE = c.EDGERC_FILE if hasattr(c, 'EDGERC_FILE') else EDGERC_FILE
    LOG_FILE = c.LOG_FILE if hasattr(c, 'LOG_FILE') else LOG_FILE
    SECTION= c.SECTION if hasattr(c, 'SECTION') else SECTION
    CONTRACTID= c.CONTRACTID if hasattr(c, 'CONTRACTID') else CONTRACTID
    GROUPID= c.GROUPID if hasattr(c, 'GROUPID') else GROUPID
    DEBUG = c.DEBUG if hasattr(c, 'DEBUG') else DEBUG
    QUIET = c.QUIET if hasattr(c, 'QUIET') else QUIET
except ImportError:
   default_vars = True

 


from logger import get_logger
LOGGER = get_logger(LOG_FILE, debug=DEBUG, quiet=QUIET)

if default_vars:
    LOGGER.debug("Using default config variables")




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


X_AKA_INFO = {
    "i":"AK_CONNECTED_CLIENT_IP",
    "e":"AK_EIP_FORWARDER_IP",
    "g":"AK_GHOST_SERVICE_IP",
    "p":"AK_CLIENT_REQUEST_NUMBER",
    "r":"AK_REGION",
    "t":"AK_CLIENT_RTT"
    }

X_ES_INFO = {
    "a":"CLIENT_ASNUM",
    "l":"CLIENT_CITY",
    "c":"CLIENT_COUNTRY_CODE",
    "x":"CLIENT_LAT",
    "y":"CLIENT_LONG"
    }

#breadcrumbs https://collaborate.akamai.com/confluence/pages/viewpage.action?spaceKey=DynamicMedia&title=Breadcrumbs+-+Leaving+a+Trail+for+Customers#BreadcrumbsLeavingaTrailforCustomers-BCtoPlayer/End-User

AKAMAI_REQUEST_BC = {
    "a":"COMPONENT_IP",
    "b":"REQ_ID",
    "c":"COMPONENT_LETTER",
    "n":"COMPONENT_LOCATION",
    "o":"COMPONENT_ASN",    
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


           
if __name__ == "__main__":
    
    if len(sys.argv) > 0 and sys.argv[1] == "test":
        l = LOGGER
        for p in sys.path:
            l.info(p)
    else:
        print("This is a configuration file, do not run directly")
        sys.exit(1)
