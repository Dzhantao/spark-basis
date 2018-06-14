package project.dao

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import project.domain.ItemsSearchClickCount
import spark.project.utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * Created by Dzhantao on 2018/6/1.
  * 从搜索引擎过来的Items点击数-数据访问层
  */
object ItemsSearchClickCountDAO {
        val tableName = "imooc_course_search_clickcount"
        val cf = "info"
        val qualifer = "click_count"

    /**
      * 保存数据到HBase
      *
      * @param list ItemsSearchClickCount集合
      */
    def save(list:ListBuffer[ItemsSearchClickCount]) :Unit ={
        val table = HBaseUtils.getInstance().getTable(tableName)

        for (ele <- list){
            table.incrementColumnValue(Bytes.toBytes(ele.day_search_item),
                Bytes.toBytes(cf),
                Bytes.toBytes(qualifer),
                ele.click_count)
        }

    }

    /**
      * 根据rowkey查询值
      */

    def count(day_search_course : String):Long ={
        val table = HBaseUtils.getInstance().getTable(tableName)

        val get = new Get(Bytes.toBytes(day_search_course))
        val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)
        if (value == null){
            0L
        }else{
            Bytes.toLong(value)
        }
    }

    def main(args: Array[String]): Unit = {
        val list = new ListBuffer[ItemsSearchClickCount]

        list.append(ItemsSearchClickCount("20171111_www.baidu.com_8",8))
        list.append(ItemsSearchClickCount("20171111_www.baidu.com_9", 9))

        save(list)

        println(count("20171111_www.baidu.com_8") + " : " + count("20171111_cn.bing.com_9"))
    }

}
