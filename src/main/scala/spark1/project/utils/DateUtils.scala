package spark1.project.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
    //输入文件日期时间格式
    val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  //目标日期格式
  //val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  /**
    *  获取时间：yyyy-MM-dd HH:mm:ss
    */
  def parse (time : String) ={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志时间：long类型
    * time: [10/Nov/2016:00:01:02 +0800]
    */
  def getTime(time: String) = {
    try {
      val timeString = time.substring(time.indexOf("[")+1,time.indexOf("]"))
//      println(timeString)
      YYYYMMDDHHMM_TIME_FORMAT.parse(timeString).getTime
    }catch {
      case  e: Exception => {
        println(e.printStackTrace())
        0L
      }
    }
  }

  def main(args: Array[String]): Unit = {
    print(parse("[10/Nov/2016:01:03:10 +0800]"))
  }
}
