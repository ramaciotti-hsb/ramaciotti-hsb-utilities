#!/usr/bin/python3

# Import required modules
import glob
import argparse
import fcsparser
import numpy
import queue
import datetime
import re
import pyodbc
import time
import os.path
from threading import Thread

parser = argparse.ArgumentParser(description='Recursively search through directories, finding and indexing .fcs files. Creates a .csv output of the resulting metadata.')
parser.add_argument('--directory', required=True, help='The directory to search inside.')
parser.add_argument('--database', help='The name of the filemaker database to use')
parser.add_argument('--output_file', help='(optional) The output file to use.')
args = parser.parse_args()

if args.output_file:
    outputFile = args.output_file
else:
    outputFile = f"../output-files/{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')}-fcs-files-index.tsv"

print('Searching for .fcs files...', flush=True)

fileAttributes = ['BEGINANALYSIS','ENDANALYSIS','BEGINSTEXT','ENDSTEXT','BEGINDATA','ENDDATA','FIL','SYS','TOT','PAR','MODE','BYTEORD','DATATYPE','NEXTDATA','CREATOR','TUBE NAME','SRC','EXPERIMENT NAME','GUID','DATE','BTIM','ETIM','CYT','FIL','SYS','TOT','PAR','MODE','BYTEORD','DATATYPE','NEXTDATA','CREATOR','TUBE NAME','SRC','EXPERIMENT NAME','GUID','DATE','BTIM','ETIM','CYT','APPLY COMPENSATION','THRESHOLD','CST SETUP STATUS','CST BEADS LOT ID','CYTOMETER CONFIG NAME','CYTOMETER CONFIG CREATE DATE','CST SETUP DATE','CST BASELINE DATE']

# Set up queue for extracting file metadata
filePathQueue = queue.Queue(maxsize=0)
num_threads = 4
threads = []
# Set up queue for writing to csv file
metadataQueue = queue.Queue(maxsize=0)

# Function for extracting metadata
def extractMetadata():
    while True:
        try:
            path = filePathQueue.get()
            print(path)
            if path is None:
                metadataQueue.put(None)
                break
            try:
                meta = fcsparser.parse(path, meta_data_only=True)
                metadataQueue.put({
                    "filePath": path,
                    "metaData": meta
                })
                print('Extracting metadata from ' + path, flush=True)
            except fcsparser.api.ParserFeatureNotImplementedError:
                print('The metadata in ' + path + ' seems to be broken, skipping...\n', flush=True)
            except ValueError:
                print('The metadata in ' + path + ' seems to be broken, skipping...\n', flush=True)
            except Exception:
                print('The metadata in ' + path + ' seems to be broken, skipping...\n', flush=True)
            filePathQueue.task_done()
        except queue.Empty:
            print("Queue was empty", flush=True)

def searchDirectories():
    for filename in glob.iglob(args.directory + '/**/*.fcs', recursive=True):
        filePathQueue.put(filename)
    for i in range(num_threads):
        filePathQueue.put(None)

for i in range(num_threads):
    worker = Thread(target=extractMetadata)
    worker.daemon = True
    worker.start()
    threads.append(worker)

globWorker = Thread(target=searchDirectories)
globWorker.daemon = True
globWorker.start()
threads.append(globWorker)

# TODO: Parameterize these database connection details
connection = pyodbc.connect("DRIVER={/Library/ODBC/FileMaker ODBC.bundle/Contents/MacOS/fmodbc.so};SERVER=127.0.0.1;PORT=2399;DATABASE=fcs-file-index;UID=python;PWD=python")
connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
connection.setencoding(encoding='utf-8')

while True:
    try:
        meta = metadataQueue.get_nowait()
        if meta is None:
            break
        print('Inserting Metadata')
        cursor = connection.cursor()
        fileRowData = {}
        metaRowData = []
        for key in meta['metaData'].keys():
            if key is not '__header__':
                if not re.search("P[0-9]", key):
                    fileRowData[key] = meta['metaData'][key]
                else:
                    metaTuple = [key, meta['metaData'][key]]
                    metaRowData.append(metaTuple)

        modifiedTime = datetime.datetime.fromtimestamp(os.path.getmtime(meta['filePath'])).strftime('%Y-%m-%d %H:%M:%S')
        createdTime = datetime.datetime.fromtimestamp(os.path.getctime(meta['filePath'])).strftime('%Y-%m-%d %H:%M:%S')

        fileLookupQuery = f"SELECT \"File ID\" FROM Files WHERE \"Full Path\" = ?"

        rows = cursor.execute(fileLookupQuery,meta['filePath']).fetchall()

        def insertMetadata(fileID):
            for metadata in metaRowData:
                # # Check if the corresponding metadata already exists
                metaDataLookupQuery = f"SELECT \"Metadata ID\" FROM Metadata WHERE \"File ID\" = ? AND \"Metadata Name\" = ?"
                metadataLookupRows = cursor.execute(metaDataLookupQuery, fileID, metadata[0]).fetchall()

                if len(metadataLookupRows) > 0:
                    metadataUpdateQuery = f"UPDATE \"Metadata\" SET \"Metadata Name\" = ?, \"Metadata Value\" = ? WHERE \"Metadata ID\" = ?"
                    cursor.execute(metadataUpdateQuery, metadata[0], metadata[1], metadataLookupRows[0][0])
                    connection.commit()
                else:
                    rowsToInsert = f"(?, ?, ?)"
                    metadataInsertQuery = f"INSERT INTO \"Metadata\" (\"File ID\", \"Metadata Name\", \"Metadata Value\") VALUES {rowsToInsert}"
                    cursor.execute(metadataInsertQuery, fileID, metadata[0], metadata[1])
                    connection.commit()

        if len(rows) > 0:
            fileID = int(rows[0][0])
            # Update the file row
            insertValues = {
                "\"Full Path\"": meta['filePath'],
                "\"File Name\"": meta['filePath'].split('/')[-1],
                "\"Created Date\"": createdTime,
                "\"Modified Date\"": modifiedTime
            }
            for validFileAttribute in fileAttributes:
                if validFileAttribute in fileRowData:
                    insertValues[f"\"{validFileAttribute}\""] = fileRowData[validFileAttribute]

            fileInsertStatements = []
            for key in insertValues.keys():
                fileInsertStatements.append(f"{key} = ?")
            # Clip the last comma
            fileInsertString = ",".join(fileInsertStatements)

            fileUpdateQuery = f"UPDATE \"Files\" SET {fileInsertString} WHERE \"File ID\" = ?"
            cursor.execute(fileUpdateQuery, *insertValues.values(), fileID)
            connection.commit()
            insertMetadata(fileID)
        else:
            insertColumns = ["\"Full Path\"", "\"File Name\"", "\"Created Date\"", "\"Modified Date\""]
            replacements = ["?", "?", "?", "?"]
            insertValues = [meta['filePath'], meta['filePath'].split('/')[-1], createdTime, modifiedTime]
            for validFileAttribute in fileAttributes:
                if validFileAttribute in fileRowData:
                    insertColumns.append("\"" + validFileAttribute + "\"")
                    replacements.append("?")
                    insertValues.append(fileRowData[validFileAttribute])
            fileInsertQuery = f"INSERT INTO \"Files\" ({', '.join(insertColumns)}) VALUES ({', '.join(replacements)})"
            cursor.execute(fileInsertQuery, insertValues)
            connection.commit()

            fileLookupQuery = f"SELECT \"File ID\" FROM Files WHERE \"Full Path\" = ?"

            rows = cursor.execute(fileLookupQuery, meta['filePath']).fetchall()

            if len(rows) > 0:
                fileID = int(rows[0][0])
                insertMetadata(fileID)

    except queue.Empty:
        time.sleep(1)

print('joining queues', flush=True)

filePathQueue.join()
metadataQueue.join()

print('Stopping workers', flush=True)


# stop workers
for t in threads:
    t.join()

print(f'Successfully wrote output file {outputFile}', flush=True)