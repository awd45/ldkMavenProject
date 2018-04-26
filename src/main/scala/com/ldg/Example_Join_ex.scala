package com.ldg

import org.apache.spark.sql.SparkSession

object Example_Join_ex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("...").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"
    var sunDb = "KOPO_REGION_MST"

    val mainDataDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val subDataDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> sunDb, "user" -> staticUser, "password" -> staticPw)).load

    mainDataDf.createOrReplaceTempView("mainTable")

    subDataDf.createOrReplaceTempView("subTable")


    var resultDf = spark.sql("SELECT A.REGIONID, B.REGIONNAME, A.PRODUCT, A.YEARWEEK, A.QTY "+
      "FROM mainTable A "+
      "LEFT JOIN subTable B " +
      "ON A.REGIONID = B.REGIONID");



/*
    var mainFile = "KOPO_CHANNEL_SEASONALITY_NEW.csv"
    var subFile = " kopo_region_mst.csv"
    var dataPath = "c:/spark/bin/data/"

    // 상대경로 입력
    var mainDataDf = spark.read.format("csv").
      option("header", "true").load(dataPath + mainFile)
    var subDataDf = spark.read.format("csv").
      option("header", "true").load(dataPath + subFile)

    mainDataDf.createOrReplaceTempView("mainTable")
    subDataDf.createOrReplaceTempView("subTable")

    //a.productgroup b.productname
    spark.sql("select a.regionid, a.product, a.yearweek, a.qty, b.regionname " +
      "from mainTable a " +
      "left join subTable b " +
      "on a.regionid = b.regionid")
*/
  }
}
