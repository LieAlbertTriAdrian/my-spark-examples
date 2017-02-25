import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.graphx._

object GraphXPageRank {
	def main (args: Array[String]) {
        val FOLLOWERS_FILE_PATH = "data/followers.txt"
        val USERS_FILE_PATH = "data/users.txt"

        val sparkConf = new SparkConf().setAppName("GraphXPageRank")
        val sparkContext = new SparkContext(sparkConf)

        val graph = GraphLoader.edgeListFile(sparkContext, FOLLOWERS_FILE_PATH)
        val ranks = graph.pageRank(0.0001).vertices
        val users = sparkContext.textFile(USERS_FILE_PATH).map { line =>
            val fields = line.split(",")

            (fields(0).toLong, fields(1))
        }
        val ranksByUsername = users.join(ranks).map {
            case (id, (username, rank)) => (username, rank)
        }        

        println(ranksByUsername.collect().mkString("\n"))

        sparkContext.stop()
    }
}