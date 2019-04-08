package mchoi.twitter

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import twitter4j.Status

object kafka_producer {

  // Kafka Topics
  val TOPIC = "twocar"

  // Kafka Properties
  val props = new Properties()
  /** * * * * * * * * * Local Specs * * * * * * * * * * * * */
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  /** * * * * * * Hortonworks Sandbox Specs * * * * * * * * */
  //  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox.hortonworks.com:6667")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  // Kafka Confuguration
  // val config = new ProducerConfig(props)

  // Defining Kafka Producer
  val kafkaProducer = new KafkaProducer[String, String](props)

  // Sending Message to Kafka
  def sendToKafka(tweet: Status): Unit = {
    val tweet_text =
       s"""${tweet.getText}, ${tweet.getId}, ${tweet.getCreatedAt}"""

    val msg = new ProducerRecord[String, String](TOPIC, tweet_text)
    kafkaProducer.send(msg)
  }

}
