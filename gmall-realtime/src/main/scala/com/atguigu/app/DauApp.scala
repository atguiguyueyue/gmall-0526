package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHanler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.将获取到的json格式数据转为样例类，并补全logDate&logHour字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        //补全logDate
        val times: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)
        //补全logHour
        startUpLog.logHour = times.split(" ")(1)
        startUpLog
      })
    })

    startUpLogDStream.cache()

    //5.做批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHanler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    filterByRedisDStream.cache()

    //原始数据条数
    startUpLogDStream.count().print()

    //经过批次间去重后的数据条数
    filterByRedisDStream.count().print()

    //6.做批次内去重

    //7.将去重的结果写入redis中
    DauHanler.saveToRedis(filterByRedisDStream)

    //8.将去重后的明细数据写入hbase


//    //4.打印kafka中的数据
//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(patition=>{
//        patition.foreach(record=>{
//          println(record.value())
//        })
//      })
//    })


    //开启任务
    ssc.start()
    //阻塞任务
    ssc.awaitTermination()
  }

}
