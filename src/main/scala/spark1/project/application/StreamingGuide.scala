package spark1.project.application

import org.apache.spark._
import org.apache.spark.streaming._
// import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

object StreamingGuide {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf,Seconds(1))

    val lines = ssc.socketTextStream("localhost",9999) // create a DStream that will connect to hostname:port, like..

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_ + _ )

    // print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()  // start the computation
    ssc.awaitTermination() // wait for the computation to terminate
  }


}
