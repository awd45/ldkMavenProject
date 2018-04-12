package com.ldk

import org.apache.spark.sql.SparkSession

object Example_Seasonality {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("...").
      config("spark.master", "local").
      getOrCreate()

    //oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"
    var productNameDb = "KOPO_PRODUCT_MASTER"

    //가져온 데이터를 아래 dataframe으로 저장하고 가져온다.
    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb, "user" -> staticUser, "password" -> staticPw)).load

    //가상의 테이블을 2개 만들어 담아준다.
    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")
  }
}

