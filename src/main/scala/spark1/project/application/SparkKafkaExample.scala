package spark1.project.application

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.spark_project.guava.eventbus.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkKafkaExample {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092, anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer"->classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false:java.lang.Boolean)
  )
  val topics = Array("topicA","topicB")
  val sparkConf = new SparkConf()
    .setAppName("CountByStreaming").setMaster("local[4]") // 部署时需删除
  //
  //    // 创建spark离散流，每隔60s接收数据
  val ssc = new StreamingContext(sparkConf, Seconds(60))
//  val stream = KafkaUtils.createDirectStream[String, String](
//    ssc,
//    PreferConsistent,
//    Subscribe[String, String](topics, kafkaParams)
//  )
//  stream.map(record => (record.key, record.value))
}
