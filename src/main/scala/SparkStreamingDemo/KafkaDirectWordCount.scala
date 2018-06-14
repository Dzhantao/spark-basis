package SparkStreamingDemo

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._


/**
  * Created by Dzhantao on 2018/5/31.
  */
object KafkaDirectWordCount {
    def main(args: Array[String]): Unit = {

        if (args.length != 2 ){
            System.err.println("Usage: KafkaReceiverWordCount <broker><topics>")
            System.exit(1)
        }

        val Array(brokers,topics) = args

        val conf = new SparkConf().setAppName("KafkaDirectWordCount")
        .setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))

        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
        //val messages = KafkaUtils.createStream(ssc, zkQuorum,group,topicMap)
        val messages = KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](
            ssc,kafkaParams,topicsSet)
        messages.map(_._2).flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).print()
        ssc.start()
        ssc.awaitTermination()
    }
}
