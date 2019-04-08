
package kin
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

object TwitterTable {


  def main(args: Array[String]): Unit = {

    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("twitter-cassandra")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val topics = List("twitterCapstone").toSet
    sc.setLogLevel("ERROR")


    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "sandbox.hortonworks.com:6667",
//      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "group.id" -> "twitterCapstone"
            "bootstrap.servers" -> "localhost:9092", // Your server
//            "key.deserializer" -> classOf[StringDeserializer],
//            "value.deserializer" -> classOf[StringDeserializer],
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "group.id" -> "kins", // Your consumer group
            "auto.offset.reset" -> "earliest"
    )

    // Getting streaming data from Kafka and send it to the Spark and create Dstream RDD
    val kafka_stream_Dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    //import implicits._

    val jSchema = StructType(
      List(
        StructField("created_at", StringType, true),
        StructField("text", StringType, true),
        StructField("user", StructType(List(
          StructField("screen_name", StringType, true),
          StructField("followers_count", IntegerType, true),
          StructField("friends_count", IntegerType, true),
          StructField("location", StringType, true))))))


    // Transformation - 1 : converting all characters to lower cases
    // from Minkyu
    val lower_Dstream = kafka_stream_Dstream.map(record => record.value.toString.toLowerCase)
    lower_Dstream.foreachRDD (rddRaw => {

      val spark = SparkSession.builder().master("local[*]").config(conf).getOrCreate()


      val df = spark.read.schema(jSchema).json(rddRaw)
        .select("created_at", "text", "user.screen_name", "user.followers_count", "user.friends_count", "user.location")
//      val recordF = df.na.fill(" ")
//        .withColumn("followers_count", col("followers_count")
//          .cast(StringType)).withColumn("friends_count", col("friends_count")
//        .cast(StringType)).filter(col("text").rlike("^[A-Za-z ][A-Za-z0-9!@#$%^& ]*$") && col("location")
//        .rlike("^[A-Za-z ][A-Za-z0-9!@#$%^& ]*$"))
      df.show()
//
//      recordF.write.mode("append").options(
////        Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
////        .format("org.apache.spark.sql.execution.datasources.hbase")
//        .save()
      df.write.mode("append").cassandraFormat("users", "twittertable").save()

      val readUsers = spark.read.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "users", "keyspace" -> "twittertable"))
        .load()

      readUsers.show()

 //spark.stop()

    }


    )
    ssc.start()
    ssc.awaitTerminationOrTimeout(40000)
    //Thread.sleep(2000)

    ssc.stop(true)
  }



}
