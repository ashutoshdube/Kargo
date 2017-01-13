import os
import sys
import h5py
from pyspark import SparkContext
from pyspark.sql import SQLContext
from itertools import combinations


def getRawRecord(x):

    """ Function to create (key, value) record """

    return (x["year"],(x["artist_name"], x["artist_hotttnesss"], x["artist_familiarity"], x["duration"] ,x["loudness"],x["tempo"]))

def getAttributeSum(x,y):

    """ Function to sum all the attributes for the year """

    output = []

    for field1, field2 in zip(x, y):
        output.append(field1 + field2)

    return output

def getAverage(year, features, count):

    """ Function to calculate average for all attributes for the year """

    featureAvg = []

    for feature in features:
        featureAvg.append(feature/count)

    return (year, featureAvg)


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

            fileDict["artist_name"] = float(len(fileObj["metadata"]["songs"]["artist_name"][0]))

            fileDict["artist_hotttnesss"] = float(fileObj["metadata"]["songs"]["artist_hotttnesss"][0])

            fileDict["artist_familiarity"] = float(fileObj["metadata"]["songs"]["artist_familiarity"][0])

            fileDict["duration"] = float(fileObj["analysis"]["songs"]["duration"][0])
            
            fileDict["loudness"] = float(fileObj["analysis"]["songs"]["loudness"][0])

            fileDict["tempo"] = float(fileObj["analysis"]["songs"]["tempo"][0])

            fileDict["year"] = float(fileObj["musicbrainz"]["songs"]["year"][0])

            output.append(fileDict)


## Get spark context
sc = SparkContext("local", "Correlation App")

## Convert python list to spark RDD
dataRdd = sc.parallelize(output)

## Filter records with year value as 0
filteredDataRdd = dataRdd.filter(lambda x : x["year"] != 0)

## Calculate the average values of attributes for the year
sumDataRdd = filteredDataRdd.map( lambda x : getRawRecord(x)).reduceByKey(lambda x,y : getAttributeSum(x, y)) 
yearCountRdd = filteredDataRdd.map( lambda x : (x["year"],1)).reduceByKey(lambda x,y : x + y)
avgRdd = sumDataRdd.join(yearCountRdd).map(lambda x : getAverage(x[0], x[1][0], x[1][1])).map(lambda x : tuple([x[0]] + x[1]))

## create dataframe for average attributes
sqlContext = SQLContext(sc)
columnNames = ["year", "artist_name", "artist_hotttnesss", "artist_familiarity", "duration", "loudness", "tempo"]
dataFrame = sqlContext.createDataFrame(avgRdd.collect(),columnNames)

## Get all field pair combinations
fieldCombinations =  list(combinations(columnNames, 2))

## Calculate correlation between all field
## combination pairs
table = {}
for field in columnNames:
    table[field] = {}

for field1, field2 in fieldCombinations:
    table[field1][field2] = dataFrame.corr(field1,field2)

print table













