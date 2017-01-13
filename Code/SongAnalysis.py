import os
import sys
import h5py
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD


def getRawRecord(x):

    """ Function to create (key, value) record """

    return (x["year"],(x["duration"],x["tempo"],x["loudness"]))

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

def createVectors(record):

    """ Create vector of Labeled points required for linear regression
        analysis """

    return LabeledPoint(record[0], record[1])


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
        	
            fileDict["year"] = fileObj["musicbrainz"]["songs"][0][1]

            fileDict["duration"] = fileObj["analysis"]["songs"][0][3]

            fileDict["loudness"] = fileObj["analysis"]["songs"]["loudness"][0]

            fileDict["tempo"] = fileObj["analysis"]["songs"]["tempo"][0]

            output.append(fileDict)

## Get spark context
sc = SparkContext("local", "Song Analysis App")

## Convert python list to spark RDD
dataRdd = sc.parallelize(output)

## Filter records with year value as 0
filteredDataRdd = dataRdd.filter(lambda x : x["year"] != 0)

## Calculate average values of attributes for the year
yearData = filteredDataRdd.map( lambda x : (x["year"],1)).reduceByKey(lambda x,y : x + y)
durationData = filteredDataRdd.map( lambda x : getRawRecord(x)).reduceByKey(lambda x,y : getAttributeSum(x, y)) 
avgDuration = durationData.join(yearData).map(lambda x : getAverage(x[0], x[1][0], x[1][1]))

## Divide the input dataset into training dataset and test dataset
trainingRdd, testRdd = avgDuration.map(lambda x : createVectors(x)).randomSplit([0.8, 0.2], 15)

## Build the regression model
model = LinearRegressionWithSGD.train(trainingRdd, iterations = 20, intercept=True, step = 0.0000333)

## Run the model on test data to get the predictions
predictions = testRdd.map(lambda x : (x.label, model.predict(x.features)))
print predictions.collect()

## Calculate average error in prediction
avgError = predictions.map(lambda (v, p): abs(v - p)/p).reduce(lambda x, y: x + y) / predictions.count() 
print("avg error = " + str(avgError))






