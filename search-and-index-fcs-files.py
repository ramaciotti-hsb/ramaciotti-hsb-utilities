#!/usr/bin/python3

# Import required modules
import glob
import argparse
import fcsparser
import numpy
import queue
import datetime
from threading import Thread

parser = argparse.ArgumentParser(description='Recursively search through directories, finding and indexing .fcs files. Creates a .csv output of the resulting metadata.')
parser.add_argument('--directory', required=True, help='The directory to search inside.')
parser.add_argument('--output_file', help='(optional) The output file to use.')
args = parser.parse_args()

if args.output_file:
    outputFile = args.output_file
else:
    outputFile = f"../output-files/{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')}-fcs-files-index.tsv"

print('Searching for .fcs files...', flush=True)

text_file = open(outputFile, "w")
text_file.write("\t".join([ 'fileName', 'metadataName', 'metadataValue' ]) + "\n")
text_file.close()

# Function for extracting metadata
def extractMetadata():
    while True:
        try:
            path = filePathQueue.get()
            if path is None:
                break
            try:
                meta = fcsparser.parse(path, meta_data_only=True)
                metadataQueue.put({
                    "fileName": path,
                    "metaData": meta
                })
                print('Extracting metadata from ' + path, flush=True)
            except fcsparser.api.ParserFeatureNotImplementedError:
                print('The metadata in ' + path + ' seems to be broken, skipping...\n', flush=True)
            except ValueError:
                print('The metadata in ' + path + ' seems to be broken, skipping...\n', flush=True)
            filePathQueue.task_done()
        except queue.Empty:
            print("Queue was empty", flush=True)

# Function for extracting metadata
def writeMetaDataToFile():
    while True:
        try:
            meta = metadataQueue.get()
            if meta is None:
                break
            for key in meta['metaData'].keys():
                if key is not '__header__':
                    text_file = open(outputFile, "a")
                    text_file.write("\t".join([ meta['fileName'], key, str(meta['metaData'][key]) ]))
                    text_file.close()
        except queue.Empty:
            print("Queue was empty", flush=True)

# Set up queue for extracting file metadata
filePathQueue = queue.Queue(maxsize=0)
num_threads = 4
threads = []
# Set up queue for writing to csv file
metadataQueue = queue.Queue(maxsize=0)

for i in range(num_threads):
    worker = Thread(target=extractMetadata)
    worker.start()
    threads.append(worker)

for i in range(num_threads):
    worker = Thread(target=writeMetaDataToFile)
    worker.start()
    threads.append(worker)

for filename in glob.iglob(args.directory + '/**/*.fcs', recursive=True):
    filePathQueue.put(filename)

filePathQueue.join()
metadataQueue.join()

# stop workers
for i in range(num_threads):
    filePathQueue.put(None)
    metadataQueue.put(None)
for t in threads:
    t.join()

print(f'Successfully wrote output file {outputFile}', flush=True)