import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.io.Source

/**
  * Created by Dzhantao on 2018/6/13.
  *
  *  用于生成streaming读取的文件数据类
  *
  */
object FileGenerater {

    def index(filerow: Int) = {
        import java.util.Random
        val rdm = new Random()
        rdm.nextInt(filerow)
    }

    def log(date: String, message: String) = {
        println(date+"-------"+message)
    }

    def getNotTime() = {
        val now:Date = new Date()
        val datetimeFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val ntime = datetimeFormat.format(now)
        ntime
    }

    def main(args: Array[String]): Unit = {
        var i = 0
        while (i < 100){
            val filename = args(0)
            val lines = Source.fromFile(filename).getLines().toList
            val filerow = lines.length
            val writer = new PrintWriter(new File("/home/centos/data/sparkStreamingtest" + i + ".txt"))
            i=i+1
            var j = 0
            while (j < i){
                writer.write(lines(index(filerow)))
                println(lines(index(filerow)))
                j = j + 1
            }
            writer.close()
            Thread sleep(5000)
            log(getNotTime(), "/home/centos/data/sparkStreamingtest" +i+".txt generated")
        }
    }
}
