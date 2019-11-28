package spark1.project.application

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer


object CountByStreamingNew {
  def main(args: Array[String]): Unit = {
    /*
    * 最终将程序打包运行 需要接收几个参数：
    * zookeeper服务器的ip，kafka消费组， 主题， 线程数
    * */

    if (args.length != 4) {
      System.err.println("Error:you need to input:<zookeeper> <group> <toplics> <threadNum>")
      System.exit(1)
    }

    val Array(zkAddress, group, topics, threadNum) = args



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

//    val topics = Array("topicA", "topicB")
//    val stream = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )
//
//    stream.map(record => (record.key, record.value))

  }
}
