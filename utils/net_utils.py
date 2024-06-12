
#!/usr/bin/env python3
from config import LOGGER
import query2_utils as q2


def to_subnet(value)->str:
    """verifies if the input is a valid IP address or subnet and returns the subnet."""
    import ipaddress
    try:
        subnet = ipaddress.ip_network(value)
        return str(subnet)
    except ValueError:
        pass

    try:
        ip = ipaddress.ip_address(value)
        if ip.version == 4:
            subnet = ipaddress.ip_network(f"{value}/32")
        else:
            subnet = ipaddress.ip_network(f"{value}/128")
        return str(subnet)
    except ValueError:
        pass

    raise ValueError(f"Invalid IP address or subnet: {value}")


def get_srv_mon_ips(country:str=None, continent:str=None, logger=LOGGER)->list:
    """return a list of GTM server monitor ip for given country or continent"""
    

    if country:
        location = f" AND l.country  = '{country}' "
    elif continent: 
        location = f" AND l.continent  = '{continent}' "
    else:
        location = ""
        
    logger.debug(f"location: {location}")
    query = f"""
        SELECT 
        distinct s.ip as ip
        FROM
            servermonitor s, MCM_regionLocation l, mcm_machines m
        WHERE
            s.ip = m.ip
        AND
            m.region = l.physicalRegion
        {location}
        ORDER BY 1
    """
    return q2.query(query,'map').values.ravel().tolist()


def _traceroute2(host_or_ip:str, logger=LOGGER)->list:
    
    from scapy.all import traceroute
    res, unans = traceroute(host_or_ip, maxttl=20)
    return res

def _traceroute(host_or_ip:str, logger=LOGGER)->list:
    import subprocess    
    import re
    trace = []
    command = ["traceroute", "-4n", host_or_ip]
    result = subprocess.run(command, capture_output=True, text=True)
    logger.debug(f"{command}]\n{result.stdout}")

   
    for line in result.stdout.split("\n"):
        line = line.replace("ms","")
        hop = line.split()
        if hop and hop[0].isdigit():
            ip = re.findall(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', line)
            logger.debug(f"hop: {hop} ip: {ip}")
            if ip == []:
                ip = [None]
            else:
                ip = list(set(ip))

            rtt = re.findall(r'\s(\d+\.\d+)\s', line)
            if rtt == []:
                rtt = [None]


            trace.append({
                "hop": hop[0],
                "ip": ip,
                "rtt": rtt,
            })
    return trace

    




def _test():
   
    print(get_srv_mon_ips(country='Malaysia'))
    trace = _traceroute('ion-terra-ff.heartbeat.boo', sudo=False)
    for  i  in enumerate(trace):
        print(i)




if __name__ == "__main__":
    _test()    
