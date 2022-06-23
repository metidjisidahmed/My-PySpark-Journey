from pyspark import SparkContext


def parseLine(x):
    fields = x.split("\t")
    # movieId , rating
    return fields[1], fields[2]


def mainScript(sc: SparkContext):
    # The gathering
    results = sc.textFile("file:///SparkCourse/ml-100k/u.data")
    # The Cleaning
    results = results.map(parseLine)
    # The filtering
    results = sorted(results.countByKey().items(), key=lambda x: x[1] , reverse=True)
    # # Key , value mapping
    # tmins_only_kv = tmins_only.map(formatkv)
    # # Reducing ( Min )
    # min_tmins_only = tmins_only_kv.reduceByKey(reduceMinTemp)
    # Display the result
    # results = results.collect()
    for result in results:
        # print(result[0] + "\t{:.2f}F".format(result[1]))
        print(result)
