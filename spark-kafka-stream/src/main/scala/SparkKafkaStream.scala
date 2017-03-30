import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object SparkKafkaStreaming {
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println(s"""
            |Usage: DirectKafkaWordCount <brokers> <topics>
            |  <brokers> is a list of one or more Kafka brokers
            |  <topics> is a list of one or more kafka topics to consume from
            |
            """.stripMargin)
            System.exit(1)
        }

        val Array(brokers, topics) = args
        println(brokers)
        println(topics)

        // Create context with 2 second batch interval
        val sparkConf = new SparkConf().setAppName("SparkKafkaStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        // Create direct kafka stream with brokers and topics
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
        println(kafkaParams)
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet)


        // val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

        // Get the lines, split them into words, count the words and print
        val lines = messages.map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
        wordCounts.print()

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }
}