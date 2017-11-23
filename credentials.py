import configparser
from typing import Dict

config = configparser.ConfigParser()
config.read('config.conf')


def postgres_creds() -> Dict:
    return dict(config['postgres'])
