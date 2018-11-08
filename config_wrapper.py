import logging
import pprint
import json
from pprint import pformat
from pprint import pprint

def readConfiguration(configFile):
    funName = "readConfiguration:"
    logging.debug(funName + " Start")
    with open(configFile) as data_file:
        data = ""
        for line in data_file:
            li = line.strip()
            if not li.startswith("#"):
                data += "{}\r\n".format(li)
        data = json.loads(data)
        config = dict((k.lower(), v) for k, v in data.iteritems())
    logging.debug(pformat(config))
    logging.debug(funName + " End")
    return config
    
class configWrapper:
	def __init__(self, config):
		self.config = config
		
	def has_config_value(self, value):
		return value in self.config
	def get_config_value(self, value):
		return self.config[value]