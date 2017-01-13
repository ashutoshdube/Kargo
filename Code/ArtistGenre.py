import os
import h5py
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
import itertools
import csv


def combinations(x):

    """ Get all combinations of artist terms"""

    return [c for c in itertools.combinations(x, 2)]


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
            fileDict["artist_id"] = fileObj["metadata"]["songs"]["artist_id"][0]
            termsList = []
            for x in fileObj["metadata"]["artist_terms"]:
                termsList.append(x)
            fileDict["terms"] = termsList           
            output.append(fileDict)

sc = SparkContext("local", "Simple App")
dataRdd = sc.parallelize(output)


## Get all distinct genres in dataset
distinctGenres = dataRdd.map(lambda x : (x["artist_id"], x["terms"])).reduceByKey(lambda x,y : x + y).map(lambda x : list(set(x[1])))

## Get all combinations of the genre pairs
genreCombinations = distinctGenres.map(combinations).flatMap(lambda x : x)

## Get count of combinations
totalCombinationCount = genreCombinations.count()

## Get occurence count for individual genre pairs
occurenceCount = genreCombinations.map(lambda x : (x, 1)).reduceByKey(lambda x, y : x + y).map(lambda x : (x[0], ((float(x[1])/totalCombinationCount)*100)))

## Select top 50 genres
topMatches = occurenceCount.top(50, lambda x : x[1])

## Write top matches to csv file
genreMatchFile = open("ArtistGenres.csv","wb")
obj = csv.writer(genreMatchFile, dialect="excel")
for x in topMatches:
    obj.writerow([str(x[0][0]),str(x[0][1]),str(x[1])])





