import h5py
import os
import sys


## Get all files
root = "/Users/Ashutosh/Documents/Kargo/MillionSongSubset/data"

output = []

for path, subdirs, files in os.walk(root):
    for name in files:
    	if(name.lower().endswith(".h5")):
            fileObj = h5py.File(os.path.join(path, name))

            fileDict = []
        	
            fileDict.append(str(fileObj["musicbrainz"]["songs"][0][1]))

            fileDict.append(str(fileObj["analysis"]["songs"]["loudness"][0]))

            fileDict.append(str(fileObj["metadata"]["songs"]["artist_hotttnesss"][0]))

            fileDict.append(str(fileObj["metadata"]["songs"]["artist_familiarity"][0]))

            output.append(fileDict)

outputFile = open("artists.csv","w")
for x in output:
    outputFile.write(",".join(x) + "\n")

