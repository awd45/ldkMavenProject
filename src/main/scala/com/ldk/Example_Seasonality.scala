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
    var productNameDb = "KOPO_PRODUCT_MST"

    //가져온 데이터를 아래 dataframe으로 저장하고 가져온다.
    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb, "user" -> staticUser, "password" -> staticPw)).load

    //가상의 메모리테이블을 2개 만들어 담아준다.
    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol," +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "a.qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd


    //(keycol, accountid, product, yearweek, qty, product_name)
    var rawExRdd = rawRdd.filter(x=>{
      var checkValid = true

      //설정 부적합 로직
      if(x.getString(3).length != 6){
        checkValid = false;
      }
      checkValid
    })

    //  랜덤 디버깅 case #1
    var x = rawRdd.first

  /// 디버깅 case #2 (타겟팅 대상 선택)
    var rawExRdd = rawRdd.filter(x=> {
      var checkValid = false
      if ((x.getString(accountidNo) == "A60") &&
        (x.getString(productNo) == "PRODUCT34") &&
        (x.getString(yearweekNo) == "201402")) {
        checkValid = true
      }
      checkValid
    })
  }
}

