
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.elasticsearch.spark.sql._
object ELTSpark {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("twitter_elk")
      .setMaster("local[*]")
      .set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val topics = List("twitterCapstone").toSet
    sc.setLogLevel("ERROR")
    val esConf = new Configuration()
    esConf.set("es.nodes","localhost")
    esConf.set("es.port","9200")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
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
    val lower_Dstream = kafka_stream_Dstream.map(record => record.value.toString.toLowerCase)
    lower_Dstream.foreachRDD (rddRaw => {

      val spark = SparkSession.builder().master("local[*]").config(conf).getOrCreate()
      val df = spark.read.schema(jSchema).json(rddRaw)
        .select("created_at", "text", "user.screen_name", "user.followers_count", "user.friends_count", "user.location")
            val recordF = df.na.fill(" ")
     recordF.saveToEs("twitter_elk/tweet")
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(100000)
    //Thread.sleep(2000)
    ssc.stop(true)
  }
}
