package spark1.project.application

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.Interval
import org.apache.spark.streaming.kafka010._

object KafkaWordCount {
  implicit val formats = DefaultFormats

  //数据格式化时需要
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCount <brokers> <groupId> <topics>")
      System.exit(1)
    }
    //    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, groupId, topics) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" ")) //将输入的每行用空格分割成一个个word


    // 对每一秒的输入数据进行reduce，然后将reduce后的数据发送给Kafka
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(1), Seconds(1), 1).foreachRDD(rdd => {
      if (rdd.count != 0) {
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        // 实例化一个Kafka生产者
        val producer = new KafkaProducer[String, String](props)
        // rdd.colect即将rdd中数据转化为数组，然后write函数将rdd内容转化为json格式
        val str = write(rdd.collect)
        // 封装成Kafka消息，topic为"result"
        val message = new ProducerRecord[String, String]("result", null, str)
        // 给Kafka发送消息
        producer.send(message)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
