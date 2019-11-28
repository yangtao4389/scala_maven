package spark1.project.application

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.codehaus.jackson.map.ext.JodaDeserializers.DateTimeDeserializer
import spark1.project.dao.CourseClickCountDao
import spark1.project.domain.{ClickLog, CourseClickCount}
import spark1.project.utils.DateUtils

import scala.collection.mutable.ListBuffer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object CountByStreaming {
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

    /*
    * 创建spark上下文
    *
    * */
    val sparkConf = new SparkConf()
      .setAppName("CountByStreaming").setMaster("local[4]") // 部署时需删除
    //
    //    // 创建spark离散流，每隔60s接收数据
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    //    // 使用kafka作为数据源
    //    val topicsMap = topics.split(",").map((_, threadNum.toInt)).toMap
    val topicsSet = topics.split(",").toSet
    //    val topics22 = Array("topicA","topicB")
    //
    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092, anotherhost:9092",
      "bootstrap.servers" -> "199.199.199.199:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      //      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //    // 创建kafka离散流， 每隔60s 消费一次kafka集群的数据
    val kafkaInputDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    //
    //    // 得到原始的日志数据
    //    val logResourcesDS = kafkaInputDS.map(record => (record.key, record.value))
    //    val logResourcesDS = kafkaInputDS.map((_, 2)).reduceByKey(_ + _)
    //    val logResourcesDS = kafkaInputDS.map(_._2)
    val logResourcesDS = kafkaInputDS.map(_.value)

    //
    val cleanDataRDD = logResourcesDS.map(line => {
      println(line)
      val splits = line.split("\t")
      println(splits)
      if (splits.length != 5) {
        ClickLog("", "", 0, 0, "")
      } else {
        val ip = splits(0)
        val time = DateUtils.parse(splits(1))
        val status = splits(3).toInt
        val referer = splits(4)
        val url = splits(2).split(" ")(1)
        var courseId = 0
        if (url.startsWith("/class")) {
          val courseIdHtml = url.split("/")(2)
          courseId = courseIdHtml.substring(0, courseIdHtml.lastIndexOf(".")).toInt
        }
        ClickLog(ip, time, courseId, status, referer)
      }
    }).filter(x => x.courseId != 0) // 过滤掉非实战课程


    /*
    * 统计数据
    * 把计算结果写进HBase
    *
    * */
    cleanDataRDD.map(line => {
      (line.time.substring(0, 8) + "_" + line.courseId, 1)
    }).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val list = new ListBuffer[CourseClickCount]
          partition.foreach(item => {
            list.append(CourseClickCount(item._1, item._2))
          })
          CourseClickCountDao.save(list)
        })
      })


    ssc.start()
    ssc.awaitTermination()

  }
}
