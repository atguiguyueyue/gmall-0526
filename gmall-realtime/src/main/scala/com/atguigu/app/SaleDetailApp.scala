package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3.分别获取orderInfo的数据以及orderDetail的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

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

        (orderInfo.id, orderInfo)
      })
    })

    val orderDetailDStream = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (orderDetail.order_id, orderDetail)
      })
    })

    //5.双流join
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.利用加缓存的方式解决因网络延迟所带来的数据丢失问题
    val noUserSaleDetail: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建Redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      partition.foreach { case (ordeId, (infoOpt, detailOpt)) =>
        val orderInfoRedisKey: String = "orderInfo:" + ordeId
        val orderDetailRedisKey: String = "orderDetail:" + ordeId


        //1.判断OrderInfo数据是存在
        if (infoOpt.isDefined) {
          //orderInfo数据存在
          val orderInfo: OrderInfo = infoOpt.get
          //2.判断orderDetail数据是否存在
          if (detailOpt.isDefined) {
            //orderDetai数据存在
            val orderDetail: OrderDetail = detailOpt.get
            //将关联上的数据组合成为SaleDetail
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            //并将SaleDetail写入结果集合
            details.add(saleDetail)
          }

          //3.将自己（orderInfo）数据写入缓存
          //用这种方式将样例类转为JSON字符串编译报错
          //          JSON.toJSONString(orderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(orderInfoRedisKey, orderInfoJson)
          //设置过期时间
          jedis.expire(orderInfoRedisKey, 100)

          //4.去对方缓存（orderDetail）中查询有没有能关联上的数据
          //先判断对方缓存中是否有对应的rediskey，有的话再将数据查询出来，并关联
          if (jedis.exists(orderDetailRedisKey)) {
            //有能关联上的数据
            val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailRedisKey)

            /* 先导import collection.JavaConverters._这个依赖，然后可以调用.asScala
            * 这个方法将java集合转为Scala集合
            * */
            for (elem <- orderDetailSet.asScala) {
              //将查询出来的OrderDetail字符串转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }
        } else {
          //orderInfo不存在
          //5.判断orderDetail是否存在
          if (detailOpt.isDefined) {
            //orderDetail存在
            val orderDetail: OrderDetail = detailOpt.get
            //6.查询对方缓存（orderInfo）中是否有能够关联上的数据
            if (jedis.exists(orderInfoRedisKey)) {
              //有能够关联上的数据
              val orderInfoStr: String = jedis.get(orderInfoRedisKey)
              //将查询出来的JSON字符串转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            } else {
              //如果没有能够关联上的数据，则将自己写入缓存
              val orderDetailStr: String = Serialization.write(orderDetail)
              jedis.sadd(orderDetailRedisKey, orderDetailStr)
              //对orderDetail数据设置过期时间
              jedis.expire(orderDetailRedisKey, 100)
            }
          }
        }
      }
      jedis.close()
      details.asScala.toIterator
    })

    //7.查询redis缓存，关联userInfo数据
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(partition => {
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //跟据rediskey查询对应的用户数据
        val userInfoRedisKey: String = "userInfo:" + saleDetail.user_id
        val userInfoStr: String = jedis.get(userInfoRedisKey)
        //将读取过来的字符串转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //8.将数据保存至ES
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_SALE_DETAIL_IDNEX + "0526", list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
