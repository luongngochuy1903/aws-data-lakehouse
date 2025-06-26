import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

INPUT_COMMENT = parser.get('input', 'comment')
INPUT_POST = parser.get('input', 'post')

BRONZE_COMMENT = parser.get('bronze', 'comment')
BRONZE_POST = parser.get('bronze', 'post')

SILVER_COMMENT = parser.get('silver', 'comment')
SILVER_POST = parser.get('silver', 'post')

GOLD_COMMENT = parser.get('gold', 'comment')
GOLD_POST = parser.get('gold', 'post')