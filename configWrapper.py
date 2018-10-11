
class configWrapper:
	def __init__(self, config):
		self.config = config
		
	def has_config_value(self, value):
		return value in self.config
	def get_config_value(self, value):
		return self.config[value]