package com.cxf.member.controller

import com.cxf.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdMemberController {
  def main(args: Array[String]): Unit = {
    //1.设置用户名
    System.setProperty("HADOOP_USER_NAME","cxf")
    //2.定义spark参数sparkConf
    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    //3.创建sparkSession：执行sparksql语句
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //4.创建sc：执行spark语句
    val sc = sparkSession.sparkContext
    //5.设置参数
    //sc.hadoopConfiguration.set("fs.defaultFS","hdfs://nameservice1")
    //sc.hadoopConfiguration.set("dfs.nameservices","nameservice1")
    //6.设置参数
    //(1)开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    //(2)开启压缩
    HiveUtil.openCompression(sparkSession)
    //(3)使用snappy压缩
    //HiveUtil.useSnappyCompression(sparkSession)
    //7.对ods层原始数据进行数据清洗存入dwd层表中




  }

}
