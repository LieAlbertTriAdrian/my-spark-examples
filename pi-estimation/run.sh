../../bin/spark-submit --master local[4] --deploy-mode client --executor-memory 1g \
--name piestimator --conf "spark.app.id=piestimator" estimate_pi.py