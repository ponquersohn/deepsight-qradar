#!/usr/bin/env python

#
#
#

from __future__ import print_function

import logging
import pprint
import json
import os
import glob
import csv
from qradar import qradarAPI
from pprint import pformat
from pprint import pprint

from deepsight_feeds import DeepSightFeeds, FeedBaseException

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

def downloadDeepsightFeed(feeds, feed_id, tmp_path, overwrite=False, deleteold=True, overwright=False):
	# check latest file for feed_id
	logger.debug("Getting feeds for feed_id : {}".format(feed_id))
	response = list(feeds.getFeedFileList(feed_id))
	# find last Base feed
	for last in reversed(response):
		# sample response [('4453', 'Base', '181.11 KiB', '10/26/2017 9:25:16 AM')]
		logger.debug("checking last {}".format(last))
		if last[1] == "Base":
			break
	else: 
		logger.error("Couldnt find Base file for feed_id {}".format(feed_id))
		raise
	# check if we already have this file
	fname = "{}/{}_{}".format(tmp_path, feed_id, last[0])
	if not (os.path.isfile(fname) and not overwrite):
		logger.debug("Need to download file {}".format(last))
		if deleteold:
			logger.debug("Deleting old files")
			for f in glob.glob("{}/{}_*".format(tmp_path, feed_id)):
				logger.debug("Deleting {}".format(f))
				os.remove(f)

		logger.debug("Getting feed {} for feed_id: {}".format(last, feed_id))
		name, date, data = feeds.getFeedFile(feed_id, last[0])
		logger.debug("Got file for feed id {} {}".format(feed_id, last))
		with open(fname, "wb") as fp:
			fp.write(data)
		return data
	else:
		logger.debug("Do not need to download file.")
		with open(fname, "r") as fp:
			return fp.read()
		return None

def doTheDirtyWork(config, feeds, mapping):
	logger.debug("Working on mapping {}".format(mapping))
	data = downloadDeepsightFeed(feeds, mapping["id"], config["global"]["tmppath"])
	fieldstoadd=[]
	with open (mapping["mappingfile"]) as f:
		fieldmap = csv.reader(filter(lambda row: row[0]!='#', f), delimiter=';', quotechar='"')
		header = fieldmap.next()
		jfm = []
		for field in fieldmap:
			# [{"element_type": "IP", "key_name": "Authorization_Server_IP_Secure"}]
			fieldstoadd+=[field[2]]
			jfm+=[{"element_type":field[1], "key_name":field[2]}]
	
	qr = qradarAPI(config["qradar"])
	ret = qr.listReferenceTables()
	ret = qr.delReferenceTable(mapping["referencemap"],waitForDelete=True)
	ret = qr.addReferenceTable(mapping["referencemap"], jfm)
	
	
	data = data.split('\n')
	csvdata = csv.reader(data)
	csvheader = csvdata.next()
	print ("fieldstoadd: {}".format(fieldstoadd))
	
	print ("all data header: {}".format(csvheader))

	
	csvdata = csv.DictReader(data[1:2], csvheader)
	print ("csvdata: {}".format(csvdata))

	intoq = []
	for row in csvdata:
		outer = row[mapping["outer_key"]]
		new_row = { key: row[key] for key in fieldstoadd }
		new_row.pop(mapping["outer_key"])
		intoq+=[{outer: new_row}]
		
	logger.debug("Spliting into chunks")
	print (intoq)
	
	chunks = [intoq[x:x + 3000] for x in xrange(0, len(intoq), 3000)]
	for idx, chunk in enumerate(chunks):
		print ('\t# - ' + str(int(idx) + 1) + ' Ingesting ' + str(len(chunk)) + ' items...') 
		qr.bulkLoadReferenceTable(mapping["referencemap"],json.dumps(chunk))
	
	
if __name__ == "__main__":
	logger = logging.getLogger('')
	logger.setLevel(logging.DEBUG)
	logger.addHandler(logging.StreamHandler())
	config = readConfiguration("qradar_config.json")
	feeds = DeepSightFeeds(username=config["deepsight"]["username"], password=config["deepsight"]["password"])
	for mapping in config["mapping"]:
		doTheDirtyWork(config, feeds, mapping)#downloadDeepsightFeed(feeds, 25, config["global"]["tmppath"])


