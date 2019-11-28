package spark1.project.application

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
/*
* Use this singleton to get or register a Broadcast variable
* */
object WordBlacklist{

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc:SparkContext): Broadcast[Seq[String]] = {
    if(instance == null){
      synchronized{
        if(instance == null){
          val wordBlacklist = Seq("a","b","c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}
