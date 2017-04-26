../../bin/spark-submit \
	--master spark://54.245.217.140:7077 \
	--deploy-mode client \
	--executor-memory 512m \
	--total-executor-cores 1 \
	--name piestimator \
	--conf "spark.app.id=piestimator" \
	estimate_pi.py