package com.kopo

import org.apache.spark.sql.SparkSession

object Example_Grammar {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    ////////////////////////// 조건 판단하기
    // 체크: 입력데이터의 연도 최대 값
    var currentYear = 2018
    var deltaYear = 0
    // 체크: 입력데이터의 연도 최소 값
    var validYear = 2015

    if(deltaYear != 0){
      validYear = currentYear - deltaYear
    }else{
      validYear
    }


    ////////////////////////// 반복하기
    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size

    for(i <-0 to priceDataSize-1){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
    }

  }

}