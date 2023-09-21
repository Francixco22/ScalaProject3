import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.pubsub.PubsubUtils
import org.apache.spark.streaming.pubsub.SparkGCPCredentials

import java.nio.charset.StandardCharsets
import scala.Console.println
import scala.Predef.println
import scala.collection.immutable.Stream.Empty.print

object PubsubToSparkStreaming {
  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder
      .appName("PubSubToSparkStreaming")
      .getOrCreate()*/

    val sparkConf = new SparkConf().setAppName("PubsubApp")
    val ssc = new StreamingContext(sparkConf, Seconds(1))


    val serviceAccountJsonKeyFilePath = "C:/Users/USUARIO/IdeaProjects/ScalaProject/src/main/scala/Files/model-journal-395918-d60561732cb2.json"
    val credentials =  SparkGCPCredentials.builder.metadataServiceAccount().build()

    val lines: DStream[String] = PubsubUtils.createStream(
      ssc,
      "model-journal-395918",
      None,
      "EnergyDemand2-sub",
      credentials, StorageLevel.MEMORY_AND_DISK_SER_2
    ).map(message => new String(message.getData(), StandardCharsets.UTF_8))
    lines.print()

    // Define the schema for your JSON data
    val jsonSchema = """{"type":"struct","fields":[{"name":"Energy","type":"string","nullable":false},{"name":"Value","type":"double","nullable":false},{"name":"Renovable","type":"string","nullable":false},{"name":"Time","type":"timestamp","nullable":false}]}"""
/*
    spark.conf.set("spark.pubsub.authentication.gcpServiceAccountPath", "gs://bucketmal/model-journal-395918-d60561732cb2.json")
    spark.conf.set("spark.pubsub.projectId", "model-journal-395918")
    spark.conf.set("spark.pubsub.subscriptionId", "EnergyDemand2-sub")
*/

    // Read data from Pub/Sub topic
    /*val pubsubDF = spark.readStream
      .format("pubsub")
      .option("subscribe", "projects/model-journal-395918/topics/EnergyDemand2")
      .load()
      .selectExpr("CAST(data AS STRING)") // Assuming "data" is the Pub/Sub message field

    // Parse the JSON data and cast it to the defined schema
    val parsedDF = pubsubDF
      .selectExpr(s"from_json(data, 'jsonSchema') as data")
      .select("data.*")*/

    // Define the path where the JSON files will be stored in HDFS
    val outputPath = "gs://bucketmal/result"

    // Write the data to a JSON file in HDFS


    // Stop the Spark session
    //spark.stop()
  }
}







