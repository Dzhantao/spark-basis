package project.dao

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import project.domain.ItemsClickCount
import spark.project.utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * Created by Dzhantao on 2018/6/1.
  *
  * 商品点击数-数据访问层
  *
  */
object ItemsClickCountDAO {

    val tableName = "items_clickcount"
    val cf = "info"
    val qualifer = "click_count"

    /**
      * 保存数据到HBase
      *
      * @param list ItemClickCount集合
      */

    def save(list: ListBuffer[ItemsClickCount]):Unit ={
        val table = HBaseUtils.getInstance().getTable(tableName)

        for (ele <- list){
            table.incrementColumnValue(Bytes.toBytes(ele.day_course),
                Bytes.toBytes(cf),
                Bytes.toBytes(qualifer),
                ele.click_count)
        }
    }


    /**
      * 根据rowkey查询值
      */
    def count(day_course:String):Long = {
        val table = HBaseUtils.getInstance().getTable(tableName)

        val get = new Get(Bytes.toBytes(day_course))
        val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

        if (value == null){
            0L
        }else{
            Bytes.toLong(value)
        }
    }

    def main(args: Array[String]): Unit = {
        val list = new ListBuffer[ItemsClickCount]
        list.append(ItemsClickCount("20171111_8",8))
        list.append(ItemsClickCount("20171111_9", 9))
        list.append(ItemsClickCount("20171111_1", 100))

        save(list)

        println(count("20171111_8") + " : " + count("20171111_9") + " : " + count("20171111_1"))

    }
}
