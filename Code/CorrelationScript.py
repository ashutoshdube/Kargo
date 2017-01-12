import os
import h5py
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from itertools import permutations

## Get all files
root = "/Users/Ashutosh/Documents/Kargo/MillionSongSubset/data"


output = []

for path, subdirs, files in os.walk(root):
    for name in files:
    	if(name.lower().endswith(".h5")):
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

            fileDict["artist_name"] = float(len(fileObj["metadata"]["songs"]["artist_name"][0]))

            fileDict["artist_hotttnesss"] = float(fileObj["metadata"]["songs"]["artist_hotttnesss"][0])

            fileDict["artist_familiarity"] = float(fileObj["metadata"]["songs"]["artist_familiarity"][0])

            fileDict["duration"] = float(fileObj["analysis"]["songs"]["duration"][0])
            
            fileDict["loudness"] = float(fileObj["analysis"]["songs"]["loudness"][0])

            fileDict["tempo"] = float(fileObj["analysis"]["songs"]["tempo"][0])

            fileDict["year"] = float(fileObj["musicbrainz"]["songs"]["year"][0])

            output.append(fileDict)

    if len(output) >= 50:
        break

sc = SparkContext("local", "Simple App")

dataRdd = sc.parallelize(output)

filteredDataRdd = dataRdd.filter(lambda x : x["year"] != 0)


def getRawRecord(x):

    return (x["year"],(x["artist_name"], x["artist_hotttnesss"], x["artist_familiarity"], x["duration"] ,x["loudness"],x["tempo"]))

def getAttributeSum(x,y):

    output = []

    for field1, field2 in zip(x, y):
        output.append(field1 + field2)

    return output

def getAverage(year, features, count):

    featureAvg = []

    for feature in features:
        featureAvg.append(feature/count)

    return (year, featureAvg)

sumDataRdd = filteredDataRdd.map( lambda x : getRawRecord(x)).reduceByKey(lambda x,y : getAttributeSum(x, y)) 
yearCountRdd = filteredDataRdd.map( lambda x : (x["year"],1)).reduceByKey(lambda x,y : x + y)

avgRdd = sumDataRdd.join(yearCountRdd).map(lambda x : getAverage(x[0], x[1][0], x[1][1])).map(lambda x : tuple([x[0]] + x[1]))



##create dataframe
sqlContext = SQLContext(sc)
columnNames = ["year", "artist_name", "artist_hotttnesss", "artist_familiarity", "duration", "loudness", "tempo"]
dataFrame = sqlContext.createDataFrame(avgRdd.collect(),columnNames)

fieldCombinations =  list(permutations(columnNames, 2))

table = {}
for field in columnNames:
    table[field] = {}

for field1, field2 in fieldCombinations:
    table[field1][field2] = dataFrame.corr(field1,field2)

print table













