package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHanler {
  /**
    * 批次间去重
    *
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
    /* val value: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
       //1.创建redis连接
       val jedis: Jedis = new Jedis("hadoop102", 6379)

       //2.查询redis中的数据
       val redisKey: String = "DAU:" + startUpLog.logDate
       val mids: util.Set[String] = jedis.smembers(redisKey)

       //3.将当前批次的mid去之前批次去重过后的mid（redis中查询出来的mid）做对比，重复的去掉
       val bool: Boolean = mids.contains(startUpLog.mid)

       jedis.close()
       !bool
     })
     value*/
    //方案二:优化，在每个分区下获取连接，以减少连接个数
    /*val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partirion => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partirion.filter(startUpLog => {
        //2.查询redis中的数据
        val redisKey: String = "DAU:" + startUpLog.logDate
        val mids: util.Set[String] = jedis.smembers(redisKey)

        //3.将当前批次的mid去之前批次去重过后的mid（redis中查询出来的mid）做对比，重复的去掉
        val bool: Boolean = mids.contains(startUpLog.mid)
        !bool
      })
      jedis.close()
      logs
    })
    value*/
    //方案三：优化，在每个批次内获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      //2.获取redis中的数据
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.将数据广播到executor端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val midRDD: RDD[StartUpLog] = rdd.filter(startUpLog => {
        !midBC.value.contains(startUpLog.mid)
      })
      jedis.close()
      midRDD
    })
    value
  }

  /**
    * 将数据写入Redis
    *
    * @param startUpLogDStream
    */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(startUpLog => {

          val redisKey: String = "DAU:" + startUpLog.logDate
          jedis.sadd(redisKey, startUpLog.mid)

        })
        //关闭连接
        jedis.close()
      })
    })

  }

}
