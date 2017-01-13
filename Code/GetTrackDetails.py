import h5py
import os
import sys
from pyspark import SparkContext


## Set the root directort and initialize the record list variable
root = "/Users/Ashutosh/Documents/Kargo/MillionSongSubset/data"
output = []

## Iterate over all the directories and subdirectories and get
## the HDF5 file names. Open the file and fetch thre required 
## from the datasets
for path, subdirs, files in os.walk(root):
    for name in files:
    	if(name.lower().endswith(".h5")):
            fileObj = h5py.File(os.path.join(path, name))

            fileDict = {}
            
            fileDict["artist_name"] = fileObj["metadata"]["songs"][0][9]
        	
            fileDict["release"] = fileObj["metadata"]["songs"][0][14]
        	
            fileDict["title"] = fileObj["metadata"]["songs"][0][18]

            fileDict["genre"] = fileObj["metadata"]["songs"][0][11]
        	
            fileDict["year"] = fileObj["musicbrainz"]["songs"][0][1]

            fileDict["duration"] = fileObj["analysis"]["songs"][0][3]
        	
            fileDict["track_id"] = fileObj["analysis"]["songs"][0][30]

            fileDict["loudness"] = fileObj["analysis"]["songs"]["loudness"][0]

            fileDict["tempo"] = fileObj["analysis"]["songs"]["tempo"][0]

            fileDict["energy"] = fileObj["analysis"]["songs"]["energy"][0]

            fileDict["danceability"] = fileObj["analysis"]["songs"]["danceability"][0]

            fileDict["artist_hotttnesss"] = float(fileObj["metadata"]["songs"]["artist_hotttnesss"][0])

            fileDict["artist_familiarity"] = float(fileObj["metadata"]["songs"]["artist_familiarity"][0])

            fileDict["artist_location"] = fileObj["metadata"]["songs"]["artist_location"][0]

            output.append(fileDict)


## Convert python list to RDD
sc = SparkContext("local", "Simple App")
fieldRdd = sc.parallelize(output)


