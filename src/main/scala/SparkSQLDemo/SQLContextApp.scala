package SparkSQLDemo

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Dzhantao on 2018/5/22.
  */
object SQLContextApp {
    def main(args: Array[String]): Unit = {
        val sess = SparkSession.builder().appName(SQLContextApp.toString).master("local[*]").getOrCreate()
        //val data = sess.read.format("json").load("file:///C:\\Users\\YYT\\Desktop\\testData.json")
        val data1 = sess.read.json("file:///C:\\Users\\Desktop\\testData.json")
        data1.printSchema()
        data1.show()


        //("mergeSchema","true")将某个文件夹下的所有子文件夹中parquet文件读出，进行合并，形成一个新的表
        // 类似于sql中join的操作
        // 但是非常的耗资源，根据需要进行操作
        //val  data2 = sess.read.option("mergeSchema","true").parquet("file:///C:\\Users\\YYT\\Desktop\\")

        //mode(SaveMode.Overwrite) 每次都是重新写入，可以忽略路径已存在的错误
        data1.coalesce(1).write.partitionBy("name").format("parquet")
            .mode("append").save("file:///C:\\Users\\Desktop\\nametest2")

        sess.stop()







    }
}
