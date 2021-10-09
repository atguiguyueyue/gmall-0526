package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.获取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //4.将用户表的数据转为样例类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })

    //5.将userInfo数据存入redis
    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(record=>{
          //将数据转为样例类，目的是为了提取Userid
          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
//          println(userInfo)
          //将数据转为JSON并写入redis
          val userInfoRedisKey: String = "userInfo:"+userInfo.id
          jedis.set(userInfoRedisKey,record.value())
        })
        jedis.close()
      })
    })
    userInfoDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
