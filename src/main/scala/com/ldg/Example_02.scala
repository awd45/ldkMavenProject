package com.ldg

import org.apache.spark
import org.apache.spark.sql.SparkSession

object Example_02 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
//
//
//    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
//    var staticUser = "kopo"
//    var staticPw = "kopo"
//    var selloutDb = "kopo_channel_seasonality"
//
//    // jdbc (java database connectivity) 연결
//    val selloutDataFromPg = spark.read.format("jdbc").
//      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
//
//    // 메모리 테이블 생성
//    selloutDataFromPg.createOrReplaceTempView("selloutTable")
//    selloutDataFromPg.show

//        var staticUrl = "jdbc:oracle:thin:@192.168.110.1:1522/XE"
//        var staticUser = "LDG"
//        var staticPw = "manager"
//        var selloutDb = "KOPO_PRODUCT_VOLUME"
//
//        val selloutDataFromOracle= spark.read.format("jdbc").
//        options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
//
//        selloutDataFromOracle.createOrReplaceTempView("selloutTable")
//        selloutDataFromOracle.show

    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_batch_season_mpara"

    // jdbc (java database connectivity) 연결
    val selloutpgParamData = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutpgParamData.createOrReplaceTempView("pgParamDataTable")
    selloutpgParamData.show


  }
}

