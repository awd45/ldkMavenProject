package com.kopo

import org.apache.spark.sql.SparkSession;

object Example_Join {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    var dataPath = "c:/spark/bin/data/"
    var mainData = "kopo_channel_seasonality_ex.csv"
    var subData = "kopo_product_mst.csv"

    // Dataframe
    var mainDataDf = spark.read.format("csv").
      option("header", "true").
      load(dataPath + mainData)

    // Dataframe
    var subDataDf = spark.read.format("csv").
      option("header", "true").
      load(dataPath + subData)

    mainDataDf.createOrReplaceTempView("mainTable")

    subDataDf.createOrReplaceTempView("subTable")

    // a.productgroup b.productid
    spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " +
      "from mainTable a " +
      "inner join subTable b " +
      "on a.productgroup = b.productid")

  }
}