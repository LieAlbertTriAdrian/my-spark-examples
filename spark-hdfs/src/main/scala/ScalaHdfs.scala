import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ScalaHdfs {
    def main (args: Array[String]) {
        if (args.length < 1) {
            System.err.println("Usage: HdfsTest <input file path> <output file path>")
            System.exit(1)            
        }

        val sparkConf = new SparkConf().setAppName("PageRank")
        val sparkContext = new SparkContext(sparkConf)

        var inputFilePath = "hdfs://127.0.1.1:9000"
        inputFilePath = inputFilePath.concat(args(0))

        var  outputFilePath = "hdfs://127.0.1.1:9000"
        outputFilePath = outputFilePath.concat(args(1))

        val textFile = sparkContext.textFile(inputFilePath)
        val counts = textFile.flatMap(line => line.split(" "))
                         .map(word => (word, 1))
                         .reduceByKey(_ + _)

        counts.saveAsTextFile(outputFilePath)
    }
}