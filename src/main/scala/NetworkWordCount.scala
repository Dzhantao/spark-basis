import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Dzhantao on 2018/6/12.
  */
object NetworkWordCount {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("NetworkWordCount")

        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val lines = ssc.socketTextStream("centos",6789)
        val  result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
        result.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
