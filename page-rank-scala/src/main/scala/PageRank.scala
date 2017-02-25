import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
	def main (args: Array[String]) {
		if (args.length < 1) {
			System.err.println("Usage: SparkPageRank <file> <iter>")
			System.exit(1)
		}
	}

    val sparkConf = new SparkConf().setAppName("PageRank")
    val sparkContext = new SparkContext(sparkConf)

	val iteration = 10
    val lines = sparkContext.textFile("test.txt", 1)

	var links = lines.map { line => 
		val parts = line.split("\\s+")
		(parts(0), parts(1))
	}
	var groupedLinks = links
						.distinct()
						.groupByKey()
						.cache()
    var ranks = groupedLinks
    				.mapValues(value => 1.0)

    for (i <- 1 to iteration) {
    	val contributions = groupedLinks
    							.join(ranks)
    							.values.flatMap { case (urls, rank) =>
    								val size = urls.size
    								urls.map( url => (url, rank / size))
    							}

    	ranks = contributions
    				.reduceByKey(_ + _)
    				.mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    sparkContext.stop()
}