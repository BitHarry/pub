#/usr/bin/env python3
import os
import sys

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_PATH, ".env")
EDGERC_FILE = os.path.join(BASE_PATH, ".edgerc")
LOG_FILE = "main.log"
SECTION='default' 
CONTRACTID='G-3OBG9M3'
GROUPID='199389'
DEBUG = True
QUIET = True

#no need to modify below this line

sys.path.append(BASE_PATH)
sys.path.append(os.path.join(BASE_PATH, "src"))
from logger import get_logger

LOGGER = get_logger(LOG_FILE, debug=DEBUG, quiet=QUIET)

           
if __name__ == "__main__":
    
    if len(sys.argv) > 0 and sys.argv[1] == "test":
        l = LOGGER
        for p in sys.path:
            l.info(p)
    else:
        print("This is a configuration file, do not run directly")
        sys.exit(1)
