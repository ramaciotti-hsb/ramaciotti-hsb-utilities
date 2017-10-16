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

# Function for extracting metadata
def extractMetadata():
    while True:
        try:
            path = filePathQueue.get()
            if path is None:
                break
            meta = fcsparser.parse(path, meta_data_only=True)
            metadataQueue.put(meta)
            filePathQueue.task_done()
        except queue.Empty:
            print("Queue was empty")

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

rows = 0

for filename in glob.iglob(args.directory + '/**/*.fcs', recursive=True):
    filePathQueue.put(filename)
    rows = rows + 1

filePathQueue.join()

# stop workers
for i in range(num_threads):
    filePathQueue.put(None)
for t in threads:
    t.join()

def drain(q):
  while True:
    try:
      yield q.get_nowait()
    except queue.Empty:  # on python 2 use Queue.Empty
      break

# Construct the output object
metaDataRows = []
metaDataToOutput = {}
for meta in drain(metadataQueue):
    metaDataRows.append(meta)

for meta in metaDataRows:
    for key in meta.keys():
        if key is not '__header__':
            metaDataToOutput.setdefault(key, [])

for key in metaDataToOutput.keys():
    for meta in metaDataRows:
        if key in meta:
            metaDataToOutput[key].append(meta[key])
        else:
            metaDataToOutput[key].append('')

print(metaDataToOutput)

text_file = open(outputFile, "w")

# Loop through and print the data to a csv
outputArray = ["\t".join(metaDataToOutput.keys())]
row = 0
while row < rows:
    rowObject = []
    for key in metaDataToOutput.keys():
        # print(len(metaDataToOutput[key]))
        rowObject.append(str(metaDataToOutput[key][row]))
    outputArray.append("\t".join(rowObject))
    row = row + 1    

text_file.write("\n".join(outputArray))
text_file.close()

print(f'Successfully wrote output file {outputFile}')