package project.domain

/**
  * Created by Dzhantao on 2018/6/4.
  * 清洗后的日志信息
  *
  * @param ip         日志访问的ip地址
  * @param time       日志访问的时间
  * @param itemId   日志访问的商品编号
  * @param statusCode 日志访问的状态码
  * @param referer    日志访问的referer
  */
 case class ClickLog(ip:String, time:String, itemId:Int, statusCode:Int, referer:String)
