from pyspark import SparkContext


def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def parseLine(x):
    fields = x.split("\t")
    # movieId , cpt =1
    return fields[1], 1


def sumTheRowWithTheSameMovieId(x, y):
    return x + y


def reverseKV(x):
    return x[1], x[0]


def addMovieName(x, moviesNames):
    y = list(x)
    y.append(moviesNames[int(y[1])])
    return tuple(y)


def mainScript(sc: SparkContext):
    # Broadcasting a global value among all the nodes
    moviesNames = sc.broadcast(loadMovieNames())
    # The gathering
    results = sc.textFile("file:///SparkCourse/ml-100k/u.data")
    # The Cleaning
    results = results.map(parseLine)
    # The Reduce ( to count )
    results = results.reduceByKey(sumTheRowWithTheSameMovieId)
    #  The reversing of key , value ( to sort )
    results = results.map(reverseKV)
    # The Sorting
    results = results.sortByKey()
    # The mapping to add the movie name
    results = results.map(lambda x: addMovieName(x, moviesNames.value))
    results = results.collect()
    for result in results:
        print(result)
