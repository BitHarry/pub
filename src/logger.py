#!/usr/bin/env python3
import logging
import sys

def get_logger(file:str=None,debug:bool=True, quiet:bool=False) -> logging.Logger:
    if file is None:
        file = __file__
        log_to_file = False
    else:
        log_to_file = True 

    logger = logging.getLogger(file)   

    if debug is True:
        formatter = logging.Formatter('%(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] [%(levelname)s] %(message)s')
        logger.setLevel(logging.DEBUG)
    else:
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    if log_to_file is True:
        file_handler = logging.FileHandler(file)
        if debug:
            file_handler.setLevel(logging.DEBUG)
        else:
            file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    if quiet is False:
        stream_handler = logging.StreamHandler(sys.stdout)
        if debug:
            stream_handler.setLevel(logging.DEBUG)
        else:
            stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    
    return logger




def test():

    
    print("TEST 1")
    print("""get_logger("test1.log", debug=True, quiet=False)""")
    LOGGER = get_logger("test1.log", debug=True, quiet=False)
    LOGGER.debug("debug message")
    LOGGER.info("info message")
    LOGGER.warning("warning message")
    LOGGER.error("error message")
    LOGGER.critical("critical message")
    LOGGER.info("end of test")
    print("TEST 2")
    print("""get_logger("test2", debug=False, quiet=False)""")
    LOGGER = get_logger("test", debug=False, quiet=False)
    LOGGER.debug("debug message")
    LOGGER.info("info message")
    LOGGER.warning("warning message")
    LOGGER.error("error message")
    LOGGER.critical("critical message")
    LOGGER.info("end of test")
    print("TEST 3")
    print("""get_logger("test3", debug=True, quiet=True)""")
    LOGGER = get_logger("test3", debug=True, quiet=True)
    LOGGER.debug("debug message")
    LOGGER.info("info message")
    LOGGER.warning("warning message")
    LOGGER.error("error message")
    LOGGER.critical("critical message")
    LOGGER.info("end of test")
    print("TEST 4")
    print("""get_logger()""")
    LOGGER = get_logger()
    LOGGER.debug("debug message")
    LOGGER.info("info message")
    LOGGER.warning("warning message")
    LOGGER.error("error message")
    LOGGER.critical("critical message")
    LOGGER.info("end of test")

if __name__ == "__main__":
    test()