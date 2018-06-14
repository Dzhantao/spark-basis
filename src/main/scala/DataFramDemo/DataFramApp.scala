package DataFramDemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by Dzhantao on 2018/5/23.
  */
object DataFramApp {
    def main(args: Array[String]): Unit = {
        val sess = SparkSession.builder().appName(DataFramApp.toString).master("local[4]").getOrCreate()


        val dataDF = sess.read.json("file:///C:\\Users\\Desktop\\testData.json")
        dataDF.printSchema()
        dataDF.show()
        dataDF.select("name").show()

        //查询某几列数据并进行计算
        dataDF.select(dataDF.col("name"),(dataDF.col("age") + 10).as("age2")).show()

        //按条件过滤
        dataDF.filter(dataDF.col("age") > 19)


        //根据某一列进行分组，然后进行聚合：select age,count(1) from table group by age
        dataDF.groupBy("age").count().show()




        sess.stop()
    }
}
