package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.分别获取orderInfo的数据以及orderDetail的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    //4.将两条流的数据分别转为样例类
    val orderInfoDStream = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全时间字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        //对手机号做脱敏操作
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "*******"

        (orderInfo.id,orderInfo)
      })
    })

    val orderDetailDStream = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (orderDetail.order_id,orderDetail)
      })
    })

    //5.双流join
//    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.利用加缓存的方式解决因网络延迟所带来的数据丢失问题
    fullJoinDStream.mapPartitions(partition=>{
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      partition.foreach{case (ordeId,(infoOpt,detailOpt))=>
          //1.判断OrderInfo数据是存在
        if (infoOpt.isDefined){
          //orderInfo数据存在
          val orderInfo: OrderInfo = infoOpt.get
          //2.判断orderDetail数据是否存在
          if (detailOpt.isDefined){
            //orderDetai数据存在
            val orderDetail: OrderDetail = detailOpt.get
            //将关联上的数据组合成为SaleDetail
            val saleDetail: SaleDetail = new SaleDetail(orderInfo,orderDetail)
            //并将SaleDetail写入结果集合
            details.add(saleDetail)
          }

          //3.将自己（orderInfo）数据写入缓存
        }


      }
      partition
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
