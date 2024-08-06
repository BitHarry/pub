import signal
import pandas
import pandas.io.sql as panda_sql
from pandas import DataFrame
import sys
import warnings
from config import LOGGER as logger

TIMEOUT=20


AGGS = """akanote.dev.query.akadns.net
brave.multi.mapnocc.query.akadns.net
boc.mapnocc.query.akadns.net
c2s.multi.dev.query.akadns.net
cobra.multi.dev.query.akadns.net
conference5.dev.query.akadns.net
crypto.multi.mapnocc.query.akadns.net
csi.multi.dev.query.akadns.net
ddc.dev.query.akadns.net
dna.dev.query.akadns.net
drm.dev.query.akadns.net
euc.dev.query.akadns.net
essl.dev.query.akadns.net
ffs.dev.query.akadns.net
freeflow.mapnocctwo.query.akadns.net
freeflow.dev.query.akadns.net
freeflow.devbl.query.akadns.net
freeflow.perfbl.query.akadns.net
flash.dev.query.akadns.net
i3.dev.query.akadns.net
iis.dev.query.akadns.net
infra.dev.query.akadns.net
ingest.dev.query.akadns.net
insight.dev.query.akadns.net
insightds.dev.query.akadns.net
internal.dev.query.akadns.net
map.dev.query.akadns.net
map.devbl.query.akadns.net
mediac.dev.query.akadns.net
mega.dev.query.akadns.net
mega.devbl.query.akadns.net
mega.svcperf.query.akadns.net
mobile.dev.query.akadns.net
mts.multi.dev.query.akadns.net
netmgmt.dev.query.akadns.net
srip.dev.query.akadns.net
storage.dev.query.akadns.net
trex.multi.dev.query.akadns.net
"""

AGG_DICT = {}

for agg in AGGS.split('\n'):
    key = agg.split(".")[0] 
    #account for multiple aggs for same network
    if key in AGG_DICT:
        key = agg.split(".")[0] + "." + agg.split(".")[1]
    AGG_DICT[key] = agg


class TimeoutError(Exception):
    pass

def handler(signum, frame):
    raise TimeoutError("Query2 timed out")

signal.signal(signal.SIGALRM, handler)

def _get_agg(agg:str, agg_dict=AGG_DICT):
    if agg in agg_dict:
        return agg_dict[agg]
    else:
        return agg

def _pyquery(sql:str, agg:str,tries:int=0, max_tries:int=3, timeout:int=TIMEOUT) -> DataFrame:
        import pyquery
        agg = _get_agg(agg)
        signal.alarm(timeout)
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                conn = pyquery.pyquery_dbapi2.connect(host=agg)
                data_frame = panda_sql.read_sql_query(sql=sql, con=conn, coerce_float=True)
                signal.alarm(0)
                return data_frame
        except TimeoutError as e:
            if tries < max_tries:
                return query(sql, agg, tries=tries+1, max_tries=max_tries, timeout=timeout)
            else:
                raise e
        except Exception as e:
            raise e

def _sql2(sql:str, agg:str,tries:int=0, max_tries:int=3, timeout:int=TIMEOUT) -> DataFrame:
    import hashlib
    bytes = sql.encode('utf-8')
    md5_hash = hashlib.md5()  
    md5_hash.update(bytes)
    hash = md5_hash.hexdigest()

    query_file = f"query_{hash}.txt"
    with open(query_file, "w") as f:
        f.write(sql)
        f.write("\n")
    agg = _get_agg(agg)
    out_file = f"query_res_{hash}.csv"

    command = [
        "sql2",
        "--host", 
        f"{agg}",
        "--queryfile", 
        f"{query_file}",
        "--csv",
        "-o", 
        f"{out_file}"
    ]
    import subprocess
    print("?")
    logger.debug(f"Running command: {command}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        return pandas.read_csv(out_file)
    else:
        err = open(out_file, "r").read()
        raise Exception(err)



def query(sql:str, agg:str,tries:int=0, max_tries:int=3, timeout:int=TIMEOUT, pyquery:bool=True) -> panda_sql.DataFrame:
    if pyquery:
        return _pyquery(sql, agg, tries=tries, max_tries=max_tries, timeout=timeout)
    else:
        return _sql2(sql, agg, tries=tries, max_tries=max_tries, timeout=timeout)
    

    
def test(sql="desc bgp_communities_brave;", agg="brave"):
    agg = _get_agg(agg)
    return query(sql, agg, pyquery=False)

if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        print(test())
    else:
        print("This file is meant to be imported, not run directly. use test argument to test it.")
    