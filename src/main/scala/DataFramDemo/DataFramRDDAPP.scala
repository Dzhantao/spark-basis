package DataFramDemo


import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Dzhantao on 2018/5/23.
  *
  * DataFrame和RDD的互操作
  */
object DataFramRDDAPP {
    def main(args: Array[String]): Unit = {
        val sess = SparkSession.builder().appName(DataFramRDDAPP.toString).master("local[3]").getOrCreate()
        val  rdd = sess.sparkContext.textFile("C:\\Users\\Desktop\\imoocinfo.txt")

        /***
          * 反射
          */

//        //导入隐式转换类
//        import sess.implicits._
//        val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt,line(1).toString,line(2).toInt)).toDF()
//        infoDF.show()
//        infoDF.createTempView("info")
//        sess.sql("select * from info where age >= 30").show()


        /***
          * 编程，row
          */

        val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1).toString, line(2).toInt, line(3).toString))
        val structType = StructType(Array(  StructField("id",IntegerType,true),
                                            StructField("name",StringType,true),
                                            StructField("age",IntegerType,true),
                                            StructField("sex", StringType, true)))
        val dataDF = sess.createDataFrame(infoRDD,structType)
        // dataDF.printSchema()
        dataDF.show(10,false)  //false 表示输出的数据原样输出，没有长度限制；如果false改为数字则表示截取的长度
        dataDF.take(2)
        dataDF.sort(dataDF.col("name").desc).show()
        dataDF.sort("name","id").show()

        //按照名字升序，按照id进行降序
        dataDF.sort(dataDF.col("name").asc, dataDF.col("id").desc).show()

        val dataDF2 = sess.createDataFrame(infoRDD, structType)

        // "right" 表示join的连接方式，默认是内连接(inner), left左连接
        dataDF.join(dataDF2,dataDF.col("name") === dataDF2.col("name") ,"right")


        sess.stop()
    }

    case class Info(id:Int ,name:String ,age:Int)
}
