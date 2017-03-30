import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object ScalaHdfsStreaming {
    def main (args: Array[String]) {
        if (args.length < 1) {
            System.err.println("Usage: HdfsTest <input dir path> <output dir path>")
            System.exit(1)            
        }

        val sparkConf = new SparkConf().setAppName("ScalaHdfsStreaming")
        val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(10))

        var inputFilePath = "hdfs://127.0.1.1:9000"
        inputFilePath = inputFilePath.concat(args(0))

        var  outputFilePath = "hdfs://127.0.1.1:9000"
        outputFilePath = outputFilePath.concat(args(1))

        val textFile = sparkStreamingContext.textFileStream(inputFilePath)
        val counts = textFile.flatMap(line => line.split(" "))
                         .map(word => (word, 1))
                         .reduceByKey(_ + _)

        counts.print()
        counts.saveAsTextFiles(outputFilePath)

        sparkStreamingContext.start()
        sparkStreamingContext.awaitTermination()
    }
}