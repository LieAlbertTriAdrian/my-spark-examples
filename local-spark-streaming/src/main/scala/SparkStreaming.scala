import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object SparkStreaming {
	def main (args: Array[String]) {
        val STREAMING_DIRECTORY_PATH = "data"

        val sparkConf = new SparkConf().setAppName("SparkStreaming")
        val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(10))

        val lines = sparkStreamingContext.textFileStream(STREAMING_DIRECTORY_PATH) 
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

        wordCounts.print()

        sparkStreamingContext.start()
        sparkStreamingContext.awaitTermination()
    }
}