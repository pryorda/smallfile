import re
import os
from optparse import OptionParser
from multiprocessing import Process, Queue, Pool
import sys
import logging
import time
import pycurl


processes = set()
max_processes = 10

# Usage
parser = OptionParser()

parser.add_option("-f", "--file-list", dest="file_list",
                  help="file list", metavar="fl")

parser.add_option("-a", "--api", dest="api",
                  help="data-storage-api", metavar="data-storage-api")

parser.add_option("-b", "--bucket", dest="bucket",
                  help="Bucket to use", metavar="BUCKET")

parser.add_option("-t", "--threads", dest="threads",
                  help="threads", metavar="threads")


(options, args) = parser.parse_args()

if not options.file_list or not options.bucket or not options.api \
                                                or not options.threads:
    parser.print_help()
    sys.exit()


# config
FILELIST = options.file_list
STORAGEAPI = options.api
BUCKET = options.bucket
max_processes = int(options.threads)


# PARCHMENT logging
LOG_FILENAME = 'dsapi_read.log'

# Set up a specific logger with our desired output level
my_logger = logging.getLogger("MyLogger")
my_handler = logging.FileHandler(LOG_FILENAME)
my_logger.setLevel(logging.DEBUG)
my_logger.addHandler(my_handler)


# Functions:
# get_file(fileUUID): reads file from data-storage-api
def get_file(fileUUID):
    fileURL = STORAGEAPI + BUCKET + "/" + str(fileUUID)
    with open(os.devnull, 'w') as fd:
        c = pycurl.Curl()
        c.setopt(c.URL, fileURL)
        c.setopt(c.WRITEDATA, fd)
        c.perform()
        responseCode = c.getinfo(c.HTTP_CODE)
        c.close()
    fd.close()
    my_logger.debug("READ:FileUUID: " + fileUUID + " ResponseCode: "
                    + str(responseCode))



with open(FILELIST) as f:
    lines = f.readlines()
f.close()
lines = [x.strip() for x in lines]

pool = Pool(processes=max_processes)
pool.map(get_file, lines)
