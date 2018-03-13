package com.lantaiyuan.main

import com.lantaiyuan.utils.elineToolUtil
import com.ltybdservice.config.ElineConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Eline2HiveApplication extends App {

  lazy val bootstrapServers = ElineConf.param.getBootstrapServers
  lazy val subscribeType = ElineConf.param.getSubscribeType

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafkagroup",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val conf=new SparkConf()
            .setAppName("Eline2HiveApplication")
            .setMaster("local[*]")
  val streamingContext=new StreamingContext(conf,Seconds(60))
  val sparkContext= streamingContext.sparkContext


  //加载kafka
  //val stream_inOutStation=KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](Array("ITS_Topic_StationIO"), kafkaParams)).filter(x=>elineToolUtil.isInOutStation(x.value())).map(record=>elineToolUtil.getElineInOutStation(record.value()))
  //val stream_inoutPark=KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](Array("ITS_Topic_ParkIO"), kafkaParams)).filter(x=>elineToolUtil.isInOutPark(x.value())).map(record=>elineToolUtil.getElineinoutPark(record.value()))
  val stream_Gps=KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](Array("ITS_Topic_GIS","ITS_Topic_GIS_HD"), kafkaParams)).filter(x=>elineToolUtil.isBusGps(x.value())).map(record=>elineToolUtil.getElineGps(record.value()))


//  stream_inOutStation.foreachRDD{ rdd=>
//    val hiveContext = new HiveContext(sparkContext)
//    import hiveContext.implicits._
//    val gpsDataFrame = rdd.toDF()
//    gpsDataFrame.registerTempTable("ods_eline_inoutstation_tmp")
//    hiveContext.sql("set hive.exec.dynamic.partition=true")
//    hiveContext.sql("set hive.exec.dynamic.partition.mode=nostrick")
//    //hiveContext.sql("insert into eline.ods_eline_inoutstation partition(citycode,workmonth,workdate) select * from ods_eline_inoutstation_tmp")
//    hiveContext.sql("select * from ods_eline_inoutstation_tmp").show()
//  }
//
//  stream_inoutPark.foreachRDD{ rdd=>
//    val hiveContext = new HiveContext(sparkContext)
//    import hiveContext.implicits._
//    val inStationDataFrame = rdd.toDF()
//    inStationDataFrame.registerTempTable("ods_eline_inoutpark_tmp")
//    hiveContext.sql("set hive.exec.dynamic.partition=true")
//    hiveContext.sql("set hive.exec.dynamic.partition.mode=nostrick")
//    //hiveContext.sql("insert into eline.ods_eline_inoutpark partition(citycode,workmonth,workdate) select * from ods_eline_inoutpark_tmp")
//    hiveContext.sql("select * from ods_eline_inoutpark_tmp").show()
//  }

  stream_Gps.foreachRDD{ rdd=>
    val hiveContext = new HiveContext(sparkContext)
    import hiveContext.implicits._
    val outStationDataFrame = rdd.toDF()
    outStationDataFrame.registerTempTable("ods_eline_gps_tmp")
    hiveContext.sql("set hive.exec.dynamic.partition=true")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nostrick")
    hiveContext.sql("insert into eline.ods_eline_gps partition(citycode,workmonth,workdate) select * from ods_eline_gps_tmp")
    //hiveContext.sql("select * from ods_eline_gps_tmp").show()
  }
  
  streamingContext.start()
  streamingContext.awaitTermination()
}
