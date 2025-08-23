import json
import os

from common import SYS_CONFIG_FILE, logger


class SysConfig:
    def __init__(self):
        self.db_host = ''
        self.db_port = '3306'
        self.db_user = ''
        self.db_password = ''
        self.db_name = ''

        self.load_config()

    def load_config(self):
        """Read cam info"""
        default_res = {
            'db_host': '',
            'db_port': '3306',
            'db_user': '',
            'db_password': '',
            'db_name': ''
        }
        if not os.path.exists(SYS_CONFIG_FILE):
            self.write_sys_config(default_res)
            return default_res

        try:
            with open(SYS_CONFIG_FILE, 'r') as f:
                config = json.load(f)
            self.db_host = config['db_host']
            self.db_port = config['db_port']
            self.db_user = config['db_user']
            self.db_password = config['db_password']
            self.db_name = config['db_name']
            return config
        except Exception as e:
            logger.error(f"Error reading cam info: {e}")
            return default_res

    def write_sys_config(self, sys_config):
        try:
            with open(SYS_CONFIG_FILE, 'w') as f:
                json.dump(sys_config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error writing cam info: {e}")