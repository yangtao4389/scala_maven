package spark1.project.application

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/*
* Use this singleton to get or register an Accumulator
* */
object DroppedWordsCounter{
  @volatile private var instance:LongAccumulator = null
  def getInstance(sc:SparkContext):LongAccumulator = {
    if(instance == null){
      synchronized{
        if(instance == null){
          instance = sc.longAccumulator("WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}
