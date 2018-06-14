import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Dzhantao on 2018/6/12.
  * spark-streaming 读取文件内容
  */
object FileWorldCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("FileWorldCount")
        val ssc = new StreamingContext(conf,Seconds(5))
        val data = ssc.textFileStream(args(0))
        val res = data.flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_)
        res.print()
        ssc.start()
        ssc.awaitTermination()

    }
}
