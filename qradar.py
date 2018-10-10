import json
import os
import sys
import time
import importlib
import logging

class qradarAPI:
	
	def __init__(self,config):
		sys.path.append(os.path.realpath('modules'))
		
		self.client_module = importlib.import_module('RestApiClient')
		self.client = self.client_module.RestApiClient(config, version='7.1')
		self.logger = logging.getLogger('')
		

	def listReferenceTables(self):
		response = self.client.call_api('reference_data/tables', 'GET')
		self.logger.debug(self.dump_response(response))
		return response
	
	def addReferenceTable(self, table, keymap):
		params = {
			'name': table,
			'key_name_types': json.dumps(keymap),
			'element_type': 'ALN'
		}
		response = self.client.call_api('reference_data/tables', 'POST', params=params)
		if (response.code == 409):
			self.logger.warning("Table already exists")
		elif(response.code >= 400):
			self.logger.error("An error occurred inserting table:")
			self.logger.debug(self.dump_response(response))
			sys.exit(-1)
		self.logger.debug(self.dump_response(response))
	
	def delReferenceTable(self, table, waitForDelete = False):
		response = self.client.call_api('reference_data/tables/{}'.format(table), 'DELETE')
		if (response.code == 404):
			self.logger.debug("Reference table not found.")
		elif (response.code >= 400):
			self.logger.error("An error occurred inserting table:")
			self.logger.debug(self.dump_response(response))
			sys.exit(-1)
		self.logger.debug(self.dump_response(response))
		if waitForDelete:
			while self.checkIfReferenceTableExists(table):
				time.sleep(0.5)
		
	def checkIfReferenceTableExists(self, table):
		response = self.client.call_api('reference_data/tables/{}'.format(table), 'GET')
		if (response.code == 404):
			self.logger.debug("Reference table not found.")
			return False
		elif (response.code >= 400):
			self.logger.error("An error occurred inserting table:")
			self.logger.debug(self.dump_response(response))
			sys.exit(-1)
		return True
	
		
	def bulkLoadReferenceTable(self, table, data):
		tab = 'reference_data/tables/bulk_load/'+table
		headers={"ContentType": "application/json"}
		print ("table: {}".format(tab))
		print ("data : {}".format(data))
		response = self.client.call_api( tab, "POST", headers=headers, data=data)
		if (response.code == 409):
			self.logger.debug(self.dump_response(response))
			print("Data already exists, using existing data")
		elif(response.code >= 400):
			print("An error occurred setting up sample data:")
			self.logger.debug(self.dump_response(response))
			sys.exit(1)
		self.logger.debug(self.dump_response(response))
		return response

	def dump_response(self,response):
		parsed_response = json.loads(response.read().decode('utf-8'))
		return json.dumps(parsed_response, indent=4)
