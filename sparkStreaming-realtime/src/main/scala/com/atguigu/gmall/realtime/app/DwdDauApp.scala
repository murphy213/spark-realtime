package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.PageLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 日活宽表
  *
  * 1. 准备实时环境
  * 2. 从Redis中读取偏移量
  * 3. 从kafka中消费数据
  * 4. 提取偏移量结束点
  * 5. 处理数据
  *     5.1 转换数据结构
  *     5.2 去重
  *     5.3 维度关联
  * 6. 写入ES
  * 7. 提交offsets
  */
object DwdDauApp {

  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //2. 从Redis中读取offset
    val topicName : String = "DWD_PAGE_LOG_TOPIC_1018"
    val groupId : String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3. 从Kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId,offsets)
    }else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)
    }

    //4. 提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5. 处理数据
    // 5.1 转换结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )

    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd => println("自我审查前: " + rdd.count())
    )
    //5.2 去重
    // 自我审查: 将页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    filterDStream.cache()
    filterDStream.foreachRDD(
      rdd => {
        println("自我审查后: " + rdd.count())
        println("----------------------------")
      }
    )

    // 第三方审查:  通过redis将当日活跃的mid维护起来,自我审查后的每条数据需要到redis中进行比对去重
    // redis中如何维护日活状态
    // 类型:
    // key :
    // value :
    // 写入API:
    // 读取API:
    // 过期:
    ssc.start()
    ssc.awaitTermination()


  }
}
