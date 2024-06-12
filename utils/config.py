from logger import get_logger
import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_PATH, ".env")
EDGERC_FILE = os.path.join(BASE_PATH, ".edgerc")
SECTION='default' 
CONTRACTID='G-3OBG9M3'
GROUPID='199389'
DEBUG = True
QUIET = True
LOGGER = get_logger("cp.log", debug=DEBUG, quiet=QUIET)
   

           