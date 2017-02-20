from pyspark import SparkContext, SparkConf
import sys

if __name__ == "__main__":
    BLANK_SPACE = " "
    APP_NAME = "Spark Word Counter"

    sparkConf = SparkConf().setAppName(APP_NAME)
    sparkContext = SparkContext(conf = sparkConf)

    textFile = sys.argv[1]
    threshold = int(sys.argv[2])
    
    tokenizedWords = sparkContext \
                        .textFile(textFile) \
                        .flatMap(lambda line: line.split(BLANK_SPACE))
    wordCounts = tokenizedWords \
                    .map(lambda word: (word, 1)) \
                    .reduceByKey(lambda v1,v2: v1 + v2)
    filteredWordCounts = wordCounts \
                            .filter(lambda pair: pair[1] >= threshold)
    filteredCharCounts = filteredWordCounts \
                            .flatMap(lambda pair: pair[0]) \
                            .map(lambda char: char) \
                            .map(lambda char: (char,1)) \
                            .reduceByKey(lambda v1,v2: v1 + v2)

    list = filteredCharCounts.collect()
    print repr(list)[1:-1]