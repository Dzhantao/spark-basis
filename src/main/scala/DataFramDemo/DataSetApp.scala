package DataFramDemo

import org.apache.spark.sql.SparkSession

/**
  * Created by Dzhantao on 2018/5/23.
  */
object DataSetApp {
    def main(args: Array[String]): Unit = {
        val sess = SparkSession.builder().appName(DataSetApp.toString).master("local[4]").getOrCreate()

        /**
          * option("header","true") csv文件中是否包含头文件，如果有，需设置为true，数据从第二行开始
          * option("inferSchema","true") 设置是否具有自动推导Schema类型的功能
          * */

        val path = "file:///D:\\常用\\大数据课程\\大数据(1)\\" +
                    "34 以慕课网日志分析为例 进入大数据 Spark SQL 的世界\\" +
                    "p1867y\\ImoocSparkSQLProject\\src\\main\\resources/ipDatabase2.csv"
        val ipData = sess.read.option("header","true").option("inferSchema","true").csv(path)

        ipData.show()

        import sess.implicits._
        //DF转换成DS（dataSet）
        val ds = ipData.as[IpInfo]

        ds.show()
    }
    case class IpInfo(ip:String , subnet:String,city:Int,country:Int)
}
