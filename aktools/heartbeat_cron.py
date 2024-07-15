#!/usr/bin/env python3
import os

import sys

this_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(this_path))
from aktools import catchpoint as cp

#print(cp.get_enumerations())
status_code, data = cp.get_data(test_id='2591077', time_delta=0.5, test_type='raw')
data.query("select * from data limit 5")