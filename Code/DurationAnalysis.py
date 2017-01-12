import os
import h5py
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from itertools import combinations
import numpy

## Get all files
root = "/Users/Ashutosh/Documents/Kargo/MillionSongSubset/data"


output = []

for path, subdirs, files in os.walk(root):
    for name in files:
    	#print name
    	if(name.lower().endswith(".h5")):
        	#print os.path.join(path, name)
            fileObj = h5py.File(os.path.join(path, name))
            fileDict = {}
        	
            metadataLength = len(fileObj["metadata"]["songs"][0])
            musicbrainzLength = len(fileObj["musicbrainz"]["songs"][0])
            analysisLength = len(fileObj["analysis"]["songs"][0])

            if len(fileObj["metadata"]["songs"]) > 1 or len(fileObj["musicbrainz"]["songs"]) > 1 or len(fileObj["analysis"]["songs"]) > 1:
        		print "metadataLength : " + str(len(fileObj["metadata"]["songs"]))
        		print "musicbrainz : " + str(len(fileObj["musicbrainz"]["songs"]))
        		print "analysisLength : " + str(len(fileObj["analysis"]["songs"]))
        		sys.exit(1)

            dataTuple = []

            fileDict["year"] = fileObj["musicbrainz"]["songs"]["year"][0]

            fileDict["duration"] = fileObj["analysis"]["songs"]["duration"][0]
        	
            output.append(fileDict)

sc = SparkContext("local", "Simple App")

dataRdd = sc.parallelize(output)

filteredDataRdd = dataRdd.filter(lambda x : x["year"] != 0)

"""durationData = filteredDataRdd.map( lambda x : (x["year"],[x["duration"]])).reduceByKey(lambda x,y : x + y).map(lambda x : str(x[0]) + "," +str(min(x[1])) + "," + 
                        str(numpy.percentile(x[1],25)) + "," +
                        str(numpy.percentile(x[1],50)) + "," +
                        str(numpy.percentile(x[1],75)) + "," +
                        str(max(x[1])))"""


#durationData = filteredDataRdd.map( lambda x : (x["year"],[x["duration"]])).reduceByKey(lambda x,y : x + y).map(lambda x : str(x[0]) + "," + ",".join([str(k) for k in x[1]]))

durationData = filteredDataRdd.map(lambda x : str(x["year"]) + "," + str(x["duration"]))

durationData.saveAsTextFile("duration")









