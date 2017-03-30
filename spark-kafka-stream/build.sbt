name := "Spark-Kafka-Streaming"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.1",
	"org.apache.spark" %% "spark-streaming" % "1.6.1",
	"org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"
)

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}