import os
import h5py
import sys
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD


## Get all files
root = "/Users/Ashutosh/Documents/Kargo/MillionSongSubset/data"


output = []

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


sc = SparkContext("local", "Simple App")
dataRdd = sc.parallelize(output)
print dataRdd.count()

filteredDataRdd = dataRdd.filter(lambda x : x["year"] != 0)


yearData = filteredDataRdd.map( lambda x : (x["year"],1)).reduceByKey(lambda x,y : x + y)


def getRawRecord(x):

    return (x["year"],(x["duration"],x["tempo"],x["loudness"]))

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

durationData = filteredDataRdd.map( lambda x : getRawRecord(x)).reduceByKey(lambda x,y : getAttributeSum(x, y)) 
avgDuration = durationData.join(yearData).map(lambda x : getAverage(x[0], x[1][0], x[1][1]))


def createVectors(record):
    return LabeledPoint(record[0], record[1])

trainingRdd, testRdd = avgDuration.map(lambda x : createVectors(x)).randomSplit([0.8, 0.2], 15)


##Model
model = LinearRegressionWithSGD.train(trainingRdd, iterations = 20, intercept=True, step = 0.0000333)

predictions = testRdd.map(lambda x : (x.label, model.predict(x.features)))
print predictions.collect()

avgError = predictions.map(lambda (v, p): abs(v - p)/p).reduce(lambda x, y: x + y) / predictions.count() 
print("avg error = " + str(avgError))






