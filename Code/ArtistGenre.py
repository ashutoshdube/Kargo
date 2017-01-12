import os
import h5py
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
import itertools
import csv

## Get all files
root = "/Users/Ashutosh/Documents/Kargo/MillionSongSubset/data"


output = []

for path, subdirs, files in os.walk(root):
    for name in files:
    	if(name.lower().endswith(".h5")):
            fileObj = h5py.File(os.path.join(path, name))
            fileDict = {}
            fileDict["artist_id"] = fileObj["metadata"]["songs"]["artist_id"][0]
            termsList = []
            for x in fileObj["metadata"]["artist_terms"]:
                termsList.append(x)
            fileDict["terms"] = termsList           
            output.append(fileDict)

sc = SparkContext("local", "Simple App")
dataRdd = sc.parallelize(output)


def combinations(x):
    return [c for c in itertools.combinations(x, 2)]

distinctGenres = dataRdd.map(lambda x : (x["artist_id"], x["terms"])).reduceByKey(lambda x,y : x + y).map(lambda x : list(set(x[1])))

genreCombinations = distinctGenres.map(combinations).flatMap(lambda x : x)

totalCombinationCount = genreCombinations.count()

occurenceCount = genreCombinations.map(lambda x : (x, 1)).reduceByKey(lambda x, y : x + y).map(lambda x : (x[0], ((float(x[1])/totalCombinationCount)*100)))

topMatches = occurenceCount.top(50, lambda x : x[1])

genreMatchFile = open("ArtistGenres.csv","wb")

obj = csv.writer(genreMatchFile, dialect="excel")


for x in topMatches:
    obj.writerow([str(x[0][0]),str(x[0][1]),str(x[1])])





