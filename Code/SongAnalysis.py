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

        	#if(metadataLength >= 10) :
            fileDict["artist_name"] = fileObj["metadata"]["songs"][0][9]
        	
        	#if(metadataLength >= 14) :
            fileDict["release"] = fileObj["metadata"]["songs"][0][14]
        	
        	#if(metadataLength >= 19) :
            fileDict["title"] = fileObj["metadata"]["songs"][0][18]

            fileDict["genre"] = fileObj["metadata"]["songs"][0][11]
        	
        	#if(musicbrainzLength >= 2) :
            fileDict["year"] = fileObj["musicbrainz"]["songs"][0][1]

        	#if(analysisLength >= 4) :
            fileDict["duration"] = fileObj["analysis"]["songs"][0][3]
        	
        	#if(analysisLength >= 31) :
            fileDict["track_id"] = fileObj["analysis"]["songs"][0][30]

            fileDict["loudness"] = fileObj["analysis"]["songs"]["loudness"][0]

            fileDict["tempo"] = fileObj["analysis"]["songs"]["tempo"][0]

            fileDict["energy"] = fileObj["analysis"]["songs"]["energy"][0]

            fileDict["danceability"] = fileObj["analysis"]["songs"]["danceability"][0]

        	
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
#avgDuration = durationData.join(yearData).map(lambda x : (x[0], x[1][0]/x[1][1])).map( lambda x : str(x[0]) + "," + str(x[1])) 
avgDuration = durationData.join(yearData).map(lambda x : getAverage(x[0], x[1][0], x[1][1]))
#avgDuration.saveAsTextFile("duration")

## def abc(a ,b):
##  print str(a) + " " + str(b)
## fileObj["metadata"].visititems(abc)
##  datasets = [item for item in fileObj["metadata"].values() if isinstance(item, h5py.Dataset)]

def createVectors(record):


    #year = record["year"]
    #attributes = []

    #attributes.append(record["tempo"])
    #attributes.append(record["duration"])
    #attributes.append(record["energy"])

    return LabeledPoint(record[0], record[1])



trainingRdd, testRdd = avgDuration.map(lambda x : createVectors(x)).randomSplit([0.8, 0.2], 15)


print "trainingRdd ==========================: " + str(trainingRdd.collect())
print "testRdd : " + str(testRdd.collect())

##Model
model = LinearRegressionWithSGD.train(trainingRdd, iterations = 20, intercept=True, step = 0.0000333)
print "Model coefficients:", str(model)

predictions = testRdd.map(lambda x : (x.label, model.predict(x.features)))
print "Predictions ==========="
print predictions.collect()

avgError = predictions.map(lambda (v, p): abs(v - p)/p).reduce(lambda x, y: x + y) / predictions.count() 
#MSE = predictions.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / predictions.count()
#print("Mean Squared Error = " + str(MSE))
print("avg error = " + str(avgError))






