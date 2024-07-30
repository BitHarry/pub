#!/usr/bin/env python3
import sys
import os
import importlib.util



def import_src(src_file:str):
    if src_file in sys.modules:
        return sys.modules[src_file]
  
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    spec = importlib.util.spec_from_file_location(src_file, os.path.join(curr_dir, "src", src_file))
    module = importlib.util.module_from_spec(spec)
    sys.modules[src_file] = module
    spec.loader.exec_module(module)
    return module


modules = []
    
modules.append(import_src("akvars.py"))
modules.append(import_src("cp_utils.py"))
modules.append(import_src("query2_utils.py"))
modules.append(import_src("utils.py"))


if __name__ == "__main__":
    from akvars import LOGGER as logger
    logger.debug("Imported modules:")
    for m in modules:
        logger.debug(m)