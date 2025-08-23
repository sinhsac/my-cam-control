import ipaddress
import platform
import re
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor

import requests

from common import logger, NOT_FOUND


def check_ezviz_port(ip, port=554, timeout=0.5):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        result = s.connect_ex((str(ip), port))
        s.close()
        return result == 0
    except Exception as e:
        logger.error(e)
        return False


def check_ezviz_http(ip, timeout=1):
    try:
        res = requests.get(f"http://{ip}:8000", timeout=timeout)
        return "ezviz" in res.text.lower() or res.status_code == 200
    except Exception as e:
        logger.error(e)
        return False


def scan_ezviz_fast(network="192.168.1.0/24", max_workers=100):
    logger.info("Scanning EZVIZ network")

    network_obj = ipaddress.IPv4Network(network, strict=False)
    ezviz_cameras = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_ezviz_port, ip): ip for ip in network_obj.hosts()}

        for future in futures:
            ip = futures[future]
            if future.result():
                logger.info("EZVIZ network found")
                ezviz_cameras.append(str(ip))

    return ezviz_cameras


def get_arp_table():
    try:
        if platform.system().lower() == "windows":
            res = subprocess.run(['arp', '-a'], capture_output=True, text=True)
        else:
            res = subprocess.run(['arp', '-a'], capture_output=True, text=True)

        str_res = res.stdout.strip()
        ips = []
        for line in str_res.splitlines():
            ip_match = re.search(r'(\d+\.\d+\.\d+\.\d+) +([a-zA-Z0-9-]+) +([a-z]+)', line)
            if not ip_match:
                continue
            items = line.split()
            if len(items) < 2:
                continue
            ips.append({'ip': ip_match.group(1), 'mac': ip_match.group(2), 'type': ip_match.group(3)})
        return ips
    except Exception as e:
        logger.error(e)
        return []

def find_by_ip(ip, ips):
    for item in ips:
        if item['ip'] == ip:
            return item
    return None

def find_ip_by_mac(target_mac='54-d6-0d-f0-07-b3'):
    ips = get_arp_table()

    target_mac = (target_mac.lower()
                  .replace(":", "-")
                  .replace(".", "-"))
    for item in ips:
        if target_mac != item['mac']:
            continue
        return item['ip']
    return NOT_FOUND
