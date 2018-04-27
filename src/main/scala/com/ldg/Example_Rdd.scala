package com.ldg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType,StructField, StructType}

object Example_Rdd {
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
    var filteredRdd = rawRdd.filter(x => {
      //bolean = true
      var checkValid = true

      //찾기 : yearweek, 인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      //비교한 후 주차정보가 53이상인 경우 레코드 삭제
      if (weekValue >= 53) {
        checkValid = false;
      }

      checkValid

      var x = rawRdd.first

      //설정 부적합 로직
      if (x.getString(3).length != 6) {
        checkValid = false;
      }

      checkValid

    // 분석대상 제품군 등록
    var productArray = Array("PRODUCT", "PRODUCT2")
    //세트 타입으로 변환
    var productSet = productArray.toSet

    var resultRdd = filteredRDD.filter(x => {
      var checkValid = true

      //데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo);

      if (productSet.contains(productInfo)) {
        checkVlid = true
      }
      checkValid
    )

      //2번째 답!
  if ((productInfo == "PRODUCT1")
    (productInfo == "PRODUCT2"))
    checkValid = true
  }
checkValid

    val finalResultDf = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("KEY", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("VOLUME", StringType),
          StructField("PRODUCT_NAME", StringType))))
    )


