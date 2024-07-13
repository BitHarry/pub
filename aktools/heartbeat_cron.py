#!/usr/bin/env python
import os
import sys

from aktools import catchpoint as cp

#print(cp.get_enumerations())

from aktools import catchpoint as cp
status_code, data = cp.get_data(folder_id='75108', time_delta=3, api_key="8D4B48E57A0DA4B90C46057514D3FE4D6905C55915F24FC9F68E9574CA2F8D6D")
status_code