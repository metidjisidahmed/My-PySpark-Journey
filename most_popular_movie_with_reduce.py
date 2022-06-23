from pyspark import SparkContext


def parseLine(x):
    fields = x.split("\t")
    # movieId , cpt =1
    return fields[1],  1
def sumTheRowWithTheSameMovieId(x , y):
    return x+y
def reverseKV(x):
    return x[1], x[0]
def mainScript(sc: SparkContext):
    # The gathering
    results = sc.textFile("file:///SparkCourse/ml-100k/u.data")
    # The Cleaning
    results = results.map(parseLine)
    # The Reduce ( to count )
    results = results.reduceByKey(sumTheRowWithTheSameMovieId)
    #  The reversing of key , value ( to sort )
    results=results.map(reverseKV)
    # The Sorting
    results=results.sortByKey()
    results = results.collect()
    for result in results:
        # print(result[0] + "\t{:.2f}F".format(result[1]))
        print(result)
