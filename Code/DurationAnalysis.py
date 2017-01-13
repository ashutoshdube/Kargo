import os
import sys
import h5py
import numpy
from pyspark import SparkContext
from pyspark.sql import SQLContext
from itertools import combinations

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

            fileDict["year"] = fileObj["musicbrainz"]["songs"]["year"][0]

            fileDict["duration"] = fileObj["analysis"]["songs"]["duration"][0]
        	
            output.append(fileDict)

## Get spark context
sc = SparkContext("local", "Simple App")

## Convert python list to spark RDD
dataRdd = sc.parallelize(output)

## Filter records with year value as 0
filteredDataRdd = dataRdd.filter(lambda x : x["year"] != 0)

## Calculate percentiles of duration data for all the years
durationData = filteredDataRdd.map( lambda x : (x["year"],[x["duration"]])).reduceByKey(lambda x,y : x + y).map(lambda x : str(x[0]) + "," +str(min(x[1])) + "," + 
                        str(numpy.percentile(x[1],25)) + "," +
                        str(numpy.percentile(x[1],50)) + "," +
                        str(numpy.percentile(x[1],75)) + "," +
                        str(max(x[1])))

## Save the percentiles to file
durationData.saveAsTextFile("duration")









