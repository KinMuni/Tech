import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf

object Consumer {


  def main (args: Array[String]): Unit = {

    val zkQuorum = "localhost:2181"
    val group = "test-consumer-group"
    val topics = "twitter-test-scala"
    val numThreads = 5

    val sparkConf = new SparkConf().setAppName("Consumer 1").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val sc = ssc.sparkContext

    sc.setLogLevel("OFF")

    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
