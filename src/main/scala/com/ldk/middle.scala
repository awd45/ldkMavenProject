package com.ldk

import org.apache.spark

object middle {
  def main(args: Array[String]): Unit = {
// 2번
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"

    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load


    selloutDf.createOrReplaceTempView("selloutTable")

    selloutDf.show(2)
  }


  //3번
  var rawData = spark.sql("select regionid, product, yearweek, qty*1.2 as qty_new" from selloutTable);


  //4번

  //5번
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
