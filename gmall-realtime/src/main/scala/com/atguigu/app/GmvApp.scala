package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GmvApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.获取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    //4.把JSON格式的数据转为样例类，并补全字段
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全时间字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        //对手机号做脱敏操作
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "*******"

        orderInfo
      })
    })

    //5.将数据写入Hbase
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
      "GMALL0526_ORDER_INFO",
      Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
      HBaseConfiguration.create,
      Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
