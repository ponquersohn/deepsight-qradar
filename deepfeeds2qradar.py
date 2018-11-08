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
import sys
from qradar import qradarAPI
from pprint import pformat
from pprint import pprint

from deepsight_feeds import DeepSightFeeds, FeedBaseException
from config_wrapper import readConfiguration
from itertools import islice

def chunks(data, SIZE=3000):
    it = iter(data)
    for i in xrange(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}


def downloadDeepsightFeed(feeds, feed_id, tmp_path, overwrite=False, deleteold=False, overwright=False):
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
        logger.error("Couldn't find Base file for feed_id {}".format(feed_id))
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
    #file = open("temp/46_4464", "r") 
    #data = file.read()
    fieldstoadd=[]
    key_to_type={}
    jfm = []
    with open (mapping["mappingfile"]) as f:
        fieldmap = csv.reader(filter(lambda row: row[0]!='#', f), delimiter=';', quotechar='"')
        header = fieldmap.next()
        
        for field in fieldmap:
            # [{"element_type": "IP", "key_name": "Authorization_Server_IP_Secure"}]
            #print (field)
            fieldstoadd+=[field[2]]
            jfm+=[{"element_type":field[1], "key_name":field[2]}]
            key_to_type[field[2]] = field[1]
    
    
    qr = qradarAPI(config["qradar"])
    
    if config["qradar"]["remove_previous_table"] == '1':
        ret = qr.delReferenceTable(mapping["referencemap"],waitForDelete=True)
    ret = qr.addReferenceTable(mapping["referencemap"], jfm)
    
    #ret = qr.listReferenceTables()
    
    data = data.split('\n')

    csvdata = csv.reader(data)
    csvheader = csvdata.next()
    csvdata = csv.DictReader(data[1:], csvheader)

    intoq = {}
    for row in csvdata:
        outer = row[mapping["outer_key"]]
        new_row = {}
        for key in fieldstoadd:
            if key_to_type[key] == "NUM" and row[key] == "":

                row[key] = "-1"
            new_row[key] = row[key]
        new_row.pop(mapping["outer_key"])
        intoq[outer]=new_row
        
    logger.debug("Spliting into chunks")
    idx = 0
    length = len(intoq)
    chunk_size = 200
    rest = length
    for chunk in chunks(intoq, chunk_size):
        rest=rest - chunk_size
        if rest < 0: 
            rest = 0
        print ('\t# - ' + str(int(idx) + 1) + ' Ingesting ' + str(len(chunk)) + ' items... still '+str(rest) + " left") 
        qr.bulkLoadReferenceTable(mapping["referencemap"],json.dumps(chunk),print_request=False)
        idx=idx+1
        
    
if __name__ == "__main__":
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    config = readConfiguration("qradar_config.json")
    feeds = DeepSightFeeds(username=config["deepsight"]["username"], password=config["deepsight"]["password"])
    for mapping in config["mapping"]:
        doTheDirtyWork(config, feeds, mapping)#downloadDeepsightFeed(feeds, 25, config["global"]["tmppath"])


