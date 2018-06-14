package SparkStreamingDemo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Dzhantao on 2018/5/29.
  *
  * Spark Streaming对接Kafka的方式一
  *
  * Recevier方式接收数据
  * kafka 1.x版本后已经抛弃这种接收方式
  */
object KafkaRecevierWordCount {
   def main(args: Array[String]): Unit = {
        if (args.length != 4 ){
            System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
        }

        val Array(zkQuorum ,group,topics,numThreads) = args

        val conf = new SparkConf().setAppName("KafkaRecevierWordCount")
         .setMaster("local[4]")
        val ssc = new StreamingContext(conf,Seconds(1))
        val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

        val topicMap1 = "test,".split(",").map((_,1)).toMap
        val messages = KafkaUtils.createStream(ssc, zkQuorum,group,topicMap)
        //val messages = KafkaUtils.createStream(ssc, "hadoop1:2181", "test", topicMap1)
        messages.map(_._2).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
        ssc.start()
        ssc.awaitTermination()
    }
}
