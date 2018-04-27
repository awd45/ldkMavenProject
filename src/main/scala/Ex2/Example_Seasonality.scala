package com.kopo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType,StructField, StructType}
import org.apache.spark.sql.Row

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

    //group by개념 분석대상을 그룹핑한다.
    var  groupRdd = filteredRdd.
    //분석 대상 키 정의 (거래처, 상품)
    groupBy(x=>{ (x.getString(accountidNo), x.getString(productNo) ) } )




    //RDD 가공
    //filteredRdd.first
    // filteredRdd = (키, 지역, 상품 ,연주차 ,거래량, 상품이름 정보)

    // 처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환한다.
    var MAXVALUE = 700000

    var mapRdd = filteredRdd.map(x=>{
      //디버깅코드 : var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 700000}).first
      //로직구현예정
      var org_qty = x.getDouble(qtyNo)
      var new_qty = org_qty

      if(new_qty > MAXVALUE){
        new_qty = MAXVALUE
      }
      // 출력 Row 키, 연주차, 거래량정보_org, 거래량정보_NEW)
      (x.getString(keyNo),
      x.getString(yearweekNo),
        org_qty,
        new_qty
      )


      //Row를 쓰려면 임포트문이 필요했다. 이것은 구글에서 검색하려면 됌
      import org.apache.spark.sql.Row
      Row(x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty
      )
    })


    // 랜덤 디버깅 필터를 걸어서 보고싶은 값을 디버깅하겠다. Case #1
    var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 700000}).first


    // 디버깅 Case #2 (타겟팅 대상 선택)
    var x = mapRdd.filter(x=>{
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