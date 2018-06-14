import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Dzhantao on 2018/6/12.
  */
object testFile {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local[4]")
        val sc = new SparkContext(conf)
        sc.textFile("file:///C:/Users/YYT/Desktop/wc1/").saveAsTextFile("file:///C:/Users/YYT/Desktop/wc/s")
    }
}
