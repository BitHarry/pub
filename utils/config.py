from logger import get_logger
import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = ""
EDGERC_FILE = ""
SECTION='default' 
CONTRACTID='G-3OBG9M3'
GROUPID='199389'
DEBUG = True
QUIET = True
LOGGER = get_logger("main.log", debug=DEBUG, quiet=QUIET)
   

           