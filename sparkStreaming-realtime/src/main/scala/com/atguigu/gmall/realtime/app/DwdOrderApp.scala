package com.atguigu.gmall.realtime.app

import java.time.{LocalDate, Period}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 订单宽表任务
  *
  * 1. 准备实时环境
  * 2. 从Redis中读取offset  * 2
  * 3. 从kakfa中消费数据 * 2
  * 4. 提取offset * 2
  * 5. 数据处理
  *    5.1 转换结构
  *    5.2 维度关联
  *    5.3 双流join
  * 6. 写入ES
  * 7. 提交offset * 2
  */
object DwdOrderApp {

  def main(args: Array[String]): Unit = {
    //1. 准备环境
    val sparconf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparconf , Seconds(5))

    //2.读取offset
    //order_info
    val orderInfoTopicName : String = "DWD_ORDER_INFO_I_1018"
    val orderInfoGroup : String = "DWD_ORDER_INFO:GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] =
        MyOffsetsUtils.readOffset(orderInfoTopicName , orderInfoGroup)
    //order_detail
    val orderDetailTopicName : String = "DWD_ORDER_DETAIL_I_1018"
    val orderDetailGroup : String = "DWD_ORDER_DETAIL_GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] =
      MyOffsetsUtils.readOffset(orderDetailTopicName ,orderDetailGroup)

    //3. 从Kafka中消费数据
    // order_info
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderInfoOffsets != null && orderInfoOffsets.nonEmpty){
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,orderInfoTopicName, orderInfoGroup,orderInfoOffsets)
    }else{
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,orderInfoTopicName, orderInfoGroup)
    }

    //order_detail
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderDetailOffsets!= null && orderDetailOffsets.nonEmpty){
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,orderDetailTopicName,orderDetailGroup,orderDetailOffsets)
    }else{
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,orderDetailTopicName,orderDetailGroup)
    }

    //4. 提取offset
    //order_info
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //order_detail
    var  orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5. 处理数据
    //5.1 转换结构
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
    //orderInfoDStream.print(100)

    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )
    //orderDetailDStream.print(100)

    //5.2 维度关联
    // order_info
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        //val orderInfoes: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        val orderInfos: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (orderInfo <- orderInfos) {
          //关联用户维度
          val uid: Long = orderInfo.user_id
          val redisUserKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          //换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          orderInfo.user_gender = gender
          orderInfo.user_age = age


          //关联地区维度
          val provinceID: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")

          //补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsoCode

          //处理日期字段
          val createTime: String = orderInfo.create_time
          val createDtHr: Array[String] = createTime.split(" ")
          val createDate: String = createDtHr(0)
          val createHr: String = createDtHr(1).split(":")(0)
          //补充到对象中
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr

          //orderInfoes.append(orderInfo)
        }
        jedis.close()
        orderInfos.iterator
      }
    )
    //orderInfoDimDStream.print(100)

    //5.3 双流Join
    // 内连接 join  结果集取交集
    // 外连接
    //   左外连  leftOuterJoin   左表所有+右表的匹配  , 分析清楚主(驱动)表 从(匹配表) 表
    //   右外连  rightOuterJoin  左表的匹配 + 右表的所有,分析清楚主(驱动)表 从(匹配表) 表
    //   全外连  fullOuterJoin   两张表的所有

    // 从数据库层面： order_info 表中的数据 和 order_detail表中的数据一定能关联成功.
    // 从流处理层面:  order_info 和  order_detail是两个流， 流的join只能是同一个批次的数据才能进行join
    //               如果两个表的数据进入到不同批次中， 就会join不成功.
    // 数据延迟导致的数据没有进入到同一个批次，在实时处理中是正常现象. 我们可以接收因为延迟导致最终的结果延迟.
    // 我们不能接收因为延迟导致的数据丢失.
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] =
        orderInfoDimDStream.map( orderInfo => (orderInfo.id , orderInfo))

    val orderDetailKVDStream: DStream[(Long, OrderDetail)] =
        orderDetailDStream.map(orderDetail => (orderDetail.order_id , orderDetail))

    //val orderJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] =
    //    orderInfoKVDStream.join(orderDetailKVDStream)

    // 解决:
    //  1. 扩大采集周期 ， 治标不治本
    //  2. 使用窗口,治标不治本 , 还要考虑数据去重 、 Spark状态的缺点
    //  3. 首先使用fullOuterJoin,保证join成功或者没有成功的数据都出现到结果中.
    //     让双方都多两步操作, 到缓存中找对的人， 把自己写到缓存中
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
            orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)




    //orderJoinDStream.print(1000)




    ssc.start()
    ssc.awaitTermination()
  }
}
