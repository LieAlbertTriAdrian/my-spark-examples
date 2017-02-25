from pyspark import SparkContext, SparkConf
import sys
import random

if __name__ == "__main__":
    BLANK_SPACE = " "
    APP_NAME = "Spark Pi Estimation"
    NUM_SAMPLES = 100

    sparkConf = SparkConf().setAppName(APP_NAME)
    sparkContext = SparkContext(conf = sparkConf)

    def inside (p) :
    	x,y = random.random(), random.random()

    	return x * x + y * y < 1

    count = sparkContext.parallelize(xrange(0, NUM_SAMPLES)) \
    			.filter(inside).count()

    print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)