from pyspark import SparkConf, SparkContext
import psutil

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)

def takeOnlyTminRows(x):
    return x[1] == 'TMIN'

def formatkv(x):
    return x[0] , (x[1:])

def reduceMinTemp(x,y):
    # print("X =",x , "Y =", y)
    return 'TMIN' , min(x[1], y[1])


def mainScript(sc: SparkContext):
    #The gathering
    lines = sc.textFile("file:///SparkCourse/1800.csv")
    # The Cleaning
    parsedLines = lines.map(parseLine)
    # The filtering
    tmins_only = parsedLines.filter(takeOnlyTminRows)
    # Key , value mapping
    tmins_only_kv = tmins_only.map(formatkv)
    # Reducing ( Min )
    min_tmins_only = tmins_only_kv.reduceByKey(reduceMinTemp)
    #Display the result
    results = min_tmins_only.collect()
    for result in results:
        # print(result[0] + "\t{:.2f}F".format(result[1]))
        print(result)
