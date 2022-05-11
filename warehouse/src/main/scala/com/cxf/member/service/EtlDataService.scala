package com.cxf.member.service

import com.alibaba.fastjson.JSONObject
import com.cxf.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlDataService {

  /**
   * 1.etl用户注册信息
   * @param sc
   * @param sparkSession
   */
  def etlMemberRegtypeLog(sc:SparkContext,sparkSession:SparkSession)={
    //(1)设置隐式转换
    import sparkSession.implicits._
    //(2)数据处理
    sc.textFile(path = "/user/mapdata/ods/memberRegtype.log")
      //1)过滤非标准json数据
      .filter(item => {
        //将json字符串转为json格式
        val obj = ParseJsonData.getJsonData(item)
        //判断是否为json格式（将非json格式数据过滤）
        obj.isInstanceOf[JSONObject]
      //2)解析json获取字段值
      }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val appkey = jsonObject.getString("appkey")
        val appregurl = jsonObject.getString("appregurl")
        val bdp_uuid = jsonObject.getString("bdp_uuid")
        val createtime = jsonObject.getString("createtime")
        val dn = jsonObject.getString("dn")
        val domain = jsonObject.getString("domain")
        val dt = jsonObject.getString("dt")
        val isranreg = jsonObject.getString("isranreg")
        val regsource = jsonObject.getString("regsource")
        val regsourceName = regsource match {
          case "1" => "PC"
          case "2" => "Mobile"
          case "3" => "App"
          case "4" => "WeChat"
          case _ => "other"
        }
        val uid = jsonObject.getIntValue("uid")
        val websiteid = jsonObject.getIntValue("websiteid")
        //返回值元组类型
        (uid,appkey,appregurl,bdp_uuid,createtime,domain,isranreg,regsource,regsourceName,websiteid,dt,dn)
      })
    })
    //转换数据格式为DF
      .toDF()
    //设置分区为1
      .coalesce(1)
    //设置写入方式
      .write.mode(SaveMode.Append)
    //写出数据到dwd.dwd_momber_regtype
      .insertInto("dwd.dwd_member_regtype")
  }

  /**
   *
   */




}
