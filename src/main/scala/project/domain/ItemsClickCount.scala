package project.domain

/**
  * Created by Dzhantao on 2018/6/1.
  *
  * @param day_course  对应的就是HBase中的rowkey，20171111_1
  * @param click_count 对应的20171111_1的访问总数
  */
case class ItemsClickCount(day_course:String, click_count:Long)
