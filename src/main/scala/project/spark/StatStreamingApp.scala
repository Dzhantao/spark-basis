package project.spark


import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.dao.{ItemsClickCountDAO, ItemsSearchClickCountDAO}
import project.domain.{ClickLog, ItemsClickCount, ItemsSearchClickCount}
import project.utils.DateUtils

import scala.collection.mutable.ListBuffer

/**
  * Created by Dzhantao on 2018/6/4.
  *
  * 使用Spark Streaming处理Kafka过来的数据
  */
object StatStreamingApp {
    def main(args: Array[String]): Unit = {
        if (args.length != 4){
            println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
            System.exit(1)
        }


        val Array(zkQuorum, groupId, topics, numThreads) = args

        val sparkConf = new SparkConf().setAppName("StatStreamingApp").setMaster("local[5]")
        val ssc = new StreamingContext(sparkConf,Seconds(60))

        val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

        val messages = KafkaUtils.createStream(ssc,zkQuorum,groupId,topicMap)

        // 测试步骤一：测试数据接收
        //messages.map(_._2).count().print

        //测试步骤二:数据清洗
        val logs = messages.map(_._2)
        val cleanData = logs.map(line =>{
            val infos = line.split("\t")
            // infos(2) = "GET /class/130.html HTTP/1.1"
            // url = /class/130.html
            val url = infos(2).split(" ")(1)
            var courseId = 0

            //把item编号拿到
            if (url.startsWith("/class")){
                val courseIdHTML = url.split("/")(2)
                courseId = courseIdHTML.substring(0,courseIdHTML.lastIndexOf(".")).toInt
            }

            ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))
        }).filter(clicklog => clicklog.itemId != 0)


        //    cleanData.print()
        // 测试步骤三：统计今天到现在为止items的访问量
        cleanData.map(x =>{
            //HBase rowkey设计: 20171111_88
            (x.time.substring(0,8) + "_" + x.itemId,1)
        }).reduceByKey(_+_).foreachRDD(rdd =>{
            rdd.foreachPartition(partitionRecords =>{
                val list = new ListBuffer[ItemsClickCount]

                partitionRecords.foreach(pair =>{
                    list.append(ItemsClickCount(pair._1,pair._2))
                })

                ItemsClickCountDAO.save(list)
            })
        })


        //测试步骤四：统计从搜索引擎过来的今天到现在为止items的访问量
        cleanData.map( x =>{

            val referer = x.referer.replaceAll("//","/")
            val splits = referer.split("/")
            var host = ""
            if (splits.length > 2){
                host = splits(1)
            }
            (host,x.itemId,x.time)
        }).filter(_._1 != "").map(x =>{
            (x._3.substring(0,8) + "_" + x._1 + "_" + x._2,1)
        }).reduceByKey(_+_).foreachRDD(rdd =>{
            rdd.foreachPartition(partitionRecords =>{
                val list = new ListBuffer[ItemsSearchClickCount]

                partitionRecords.foreach(pair =>{
                    list.append(ItemsSearchClickCount(pair._1,pair._2))
                })

                ItemsSearchClickCountDAO.save(list)
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
