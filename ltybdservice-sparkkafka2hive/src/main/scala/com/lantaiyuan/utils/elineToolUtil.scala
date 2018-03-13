package com.lantaiyuan.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONException}
import com.lantaiyuan.common._

object elineToolUtil {

  val sdf = new SimpleDateFormat("yyyyMM")
  val sd = new SimpleDateFormat("yyyyMMdd")
  val sd1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def isValidDate(str: String): Boolean = {
    var convertSuccess: Boolean = true
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      format.setLenient(false)
      format.parse(str)
    } catch {
      case ex: ParseException => convertSuccess = false
    }

    convertSuccess
  }

  def isInOutStation(row:Any):Boolean= {
    var re = true
    try {
      val json = JSON.parseObject(JSON.parseObject(row.toString).get("body").toString)
      val lens = json.size()
      if (lens != 24) {
        re = false
      } else {
        val in_time = json.get("in_time").toString()
        if (in_time != null && isValidDate(in_time))
          re = true
        else
          re = false
      }
    }catch {
      case e: JSONException=>re=false
    }
    re
  }

  def isInOutPark(row:Any):Boolean= {
    var re = true
    try {
      val json = JSON.parseObject(JSON.parseObject(row.toString).get("body").toString)
      val lens = json.size()
      if (lens != 21) {
        re = false
      } else {
        val in_time = json.get("in_time").toString()
        if (in_time != null && isValidDate(in_time))
          re = true
        else
          re = false
      }
    }catch {
      case e: JSONException=>re=false
    }
    re
  }

  def isBusGps(row:Any):Boolean= {
    var re = true
    try {
      val json = JSON.parseObject(JSON.parseObject(row.toString).get("body").toString)
      val lens = json.size()
      if (lens != 17) {
        re = false
      } else {
        re = true
      }
    }catch {
      case e: JSONException=>re=false
    }
    re
  }
  

  def anyToString(obj:AnyRef):String={
    if(obj==null)
      ""
    else
      obj.toString
    
  }

  def timesToDate(timeStamp:String):String={
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sd = sdf.format(new Date(timeStamp.toLong))
    sd
  }

  def getWorkMonth():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    var hehe = dateFormat.format( now )
    hehe
  }

  def getWorkDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var hehe = dateFormat.format( now )
    hehe
  }


  def getElineInOutStation(row:String):eline_inOutStation={
    val jsonstr = JSON.parseObject(row)
    val dev_sn=anyToString(jsonstr.get("dev_sn"))
    val dev_id=anyToString(jsonstr.get("dev_id"))
    val city_code=anyToString(jsonstr.get("city_code"))
    val company_code=anyToString(jsonstr.get("company_code"))
    val line_id=anyToString(jsonstr.get("line_id"))
    val in_time = jsonstr.get("in_time").toString();
    val workmonth = sdf.format(sd1.parse(in_time))
    val workdate = sd.format(sd1.parse(in_time))
    val out_time=anyToString(jsonstr.get("out_time"))
    val lon=anyToString(jsonstr.get("lon"))
    val lat=anyToString(jsonstr.get("lat"))
    val speed=anyToString(jsonstr.get("speed"))
    val distance=anyToString(jsonstr.get("distance"))
    val angle=anyToString(jsonstr.get("angle"))
    val altitude=anyToString(jsonstr.get("altitude"))
    val vehicle_status=anyToString(jsonstr.get("vehicle_status"))
    val bus_station_code=anyToString(jsonstr.get("bus_station_code"))
    val bus_station_no=anyToString(jsonstr.get("bus_station_no"))
    val driver_id=anyToString(jsonstr.get("driver_id"))
    val station_flag=anyToString(jsonstr.get("station_flag"))
    val station_report=anyToString(jsonstr.get("station_report"))
    val dis_next=anyToString(jsonstr.get("dis_next"))
    val time_next=anyToString(jsonstr.get("time_next"))
    val next_station_no=anyToString(jsonstr.get("next_station_no"))
    val in_out_flag=anyToString(jsonstr.get("in_out_flag"))
    val direction=anyToString(jsonstr.get("direction"))
    eline_inOutStation(dev_sn,dev_id,company_code,line_id,in_time,out_time,lon,lat,speed,distance,angle,altitude,vehicle_status,bus_station_code,bus_station_no,driver_id,station_flag,station_report,dis_next,time_next,next_station_no,in_out_flag,direction,city_code,workmonth,workdate)
  }

  def getElineinoutPark(row:String):eline_inoutPark={
    val jsonstr = JSON.parseObject(row)
    val dev_sn = anyToString(jsonstr.get("dev_sn"))
    val dev_id = anyToString(jsonstr.get("dev_id"))
    val city_code = anyToString(jsonstr.get("city_code"))
    val company_code = anyToString(jsonstr.get("company_code"))
    val line_id = anyToString(jsonstr.get("line_id"))
    val in_time = jsonstr.get("in_time").toString();
    val workmonth = sdf.format(sd1.parse(in_time))
    val workdate = sd.format(sd1.parse(in_time))
    val out_time = anyToString(jsonstr.get("out_time"))
    val driver_id = anyToString(jsonstr.get("driver_id"))
    val lon = anyToString(jsonstr.get("lon"))
    val lat = anyToString(jsonstr.get("lat"))
    val speed = anyToString(jsonstr.get("speed"))
    val distance = anyToString(jsonstr.get("distance"))
    val angle = anyToString(jsonstr.get("angle"))
    val altitude = anyToString(jsonstr.get("altitude"))
    val vehicle_status = anyToString(jsonstr.get("vehicle_status"))
    val in_out_flag = anyToString(jsonstr.get("in_out_flag"))
    val direction = anyToString(jsonstr.get("direction"))
    val field_no = anyToString(jsonstr.get("field_no"))
    val field_property = anyToString(jsonstr.get("field_property"))
    val oil_volume = anyToString(jsonstr.get("oil_volume"))
    val power_consume = anyToString(jsonstr.get("power_consume"))
    eline_inoutPark(dev_sn, dev_id, company_code, line_id, in_time, out_time, driver_id, lon, lat, speed, distance, angle,altitude, vehicle_status, in_out_flag, direction, field_no,field_property,oil_volume,power_consume,city_code,workmonth, workdate)
  }

  def getElineGps(row:String):eline_gps={
    val jsonstr = JSON.parseObject(JSON.parseObject(row).get("body").toString)
    val dev_sn = anyToString(jsonstr.get("dev_sn"))
    val dev_id = anyToString(jsonstr.get("dev_id"))
    val city_code = anyToString(jsonstr.get("city_code"))
    val company_code = anyToString(jsonstr.get("company_code"))
    val line_id = anyToString(jsonstr.get("line_id"))
    val gps_time = anyToString(jsonstr.get("gps_time"));
    val workmonth = getWorkMonth()
    val workdate = getWorkDate()
    val lon = anyToString(jsonstr.get("lon"))
    val lat = anyToString(jsonstr.get("lat"))
    val speed = anyToString(jsonstr.get("speed"))
    val distance = anyToString(jsonstr.get("distance"))
    val angle = anyToString(jsonstr.get("angle"))
    val altitude = anyToString(jsonstr.get("altitude"))
    val direction = anyToString(jsonstr.get("direction"))
    val dis_next = anyToString(jsonstr.get("dis_next"))
    val time_next = anyToString(jsonstr.get("time_next"))
    val next_station_no = anyToString(jsonstr.get("next_station_no"))
    val vehicle_status = anyToString(jsonstr.get("vehicle_status"))
    eline_gps(dev_sn,dev_id,company_code,line_id,gps_time,lon,lat,speed,distance,angle,altitude,direction,dis_next,time_next,next_station_no,vehicle_status,city_code,workmonth,workdate)

  }

}
