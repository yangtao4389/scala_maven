package spark1.project.dao

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import spark1.project.domain.CourseClickCount
import spark1.project.utils.HbaseUtils


import scala.collection.mutable.ListBuffer

object CourseClickCountDao {
  var tableName = "ns1:courses_clickcount"
  var cf = "info" // 列族
  var qualifer = "click_count" // 列

  /*
  * 保存到Hbase
  * */
  def save(list:ListBuffer[CourseClickCount]):Unit = {
    val table = HbaseUtils.getInstance().getTable(tableName)
    for(item <- list){
      // 调用hbase的一个自增加方法
      table.incrementColumnValue(Bytes.toBytes(item.day_course), Bytes.toBytes(cf), Bytes.toBytes(qualifer), item.click_count)

    }
  }

  /*
  * 根据rowkey查询值
  * */
  def count(day_course:String):Long = {
    val table = HbaseUtils.getInstance().getTable(tableName)
    var get = new Get(Bytes.toBytes(day_course))
    var value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }


  def main(args: Array[String]): Unit = {
    var list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8",8))
    list.append(CourseClickCount("20171111_9",10))
    list.append(CourseClickCount("20171111_10",100))

    save(list)
    println(count("20171111_8") + " : " + count("20171111_9")+ " : " + count("20171111_10"))
  }


}
