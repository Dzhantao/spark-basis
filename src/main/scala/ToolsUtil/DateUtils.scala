package ToolsUtil


import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by Dzhantao on 2018/5/24.
  *
  * 注意：SimpleDateFormat是线程不安全
  */
object DateUtils {

    //输入文件日期时间格式
    //10/Nov/2016:00:01:02 +0800
    val YYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

    //目标日期格式
    val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


    /**
      * 获取时间： yyyy-MM-dd HH:mm:ss
      * */

    def parse(time:String) ={
        TARGET_FORMAT.format(new Date(getTime(time)))
    }


    /**
      * 获取输入日志时间：long类型
      * */

    def getTime(time:String) ={
        try{
            YYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
        }catch {
            case e:Exception =>{
                0l
            }
        }

    }

    def main(args: Array[String]): Unit = {
            println(parse("[10/Nov/2016:00:01:02 +0800]"))
    }
}
