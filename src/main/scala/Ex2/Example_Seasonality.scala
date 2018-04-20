package com.kopo

import org.apache.spark.sql.SparkSession

object Example_Seasonality {
  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("product_name")

    // (kecol, accountid, product, yearweek, qty, product_name)
    var rawRdd = rawData.rdd

    var filteredRdd = rawRdd.filter(x=>{
      // boolean = true
      var checkValid = true
      // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
      if( weekValue >= 53){
        checkValid = false
      }

      checkValid
    })

    // 미미



    // 디버깅 ase #2 (타겟팅 대상 선택)
    var rawExRdd2 = rawRdd.filter(x=>{
      var checkValid = false
      if( (x.getString(accountidNo) == "A60") &&
        (x.getString(productNo) == "PRODUCT34") &&
        (x.getString(yearweekNo) == "201402")){
        checkValid = true
      }
      checkValid
    })

    // 디버깅 Case #2 (타겟팅 대상 선택)
    var rawExRdd2 = rawRdd.filter(x=>{
      var checkValid = false
      if( (x.getString(accountidNo) == "A60") &&
        (x.getString(productNo) == "PRODUCT34") &&
        (x.getString(yearweekNo) == "201402")){
        checkValid = true
      }
      checkValid
    })


  }

}