../../bin/spark-submit \
	--class "SparkPi" \
	--master spark://alberttriadrian:7077 \
	--deploy-mode client \
	--name piestimatorscala \
	--total-executor-cores 1 \
	--executor-memory 512m \
	target/scala-2.10/spark-pi_2.10-1.0.jar

