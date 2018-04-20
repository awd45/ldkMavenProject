package com.kopo

object Example_DataType {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    // spark 정의
    var spark = SparkSession.builder().appName("hkProject").
      config("spark.master","local").
      getOrCreate()

    // Scala 사칙연산
    var intValue = 10
    var doubleValue =3.0
    // 1. 더하기 연산
    var sumValue = intValue + doubleValue
    // 2. 곱하기 연산
    var mulipleValue = intValue * doubleValue
    // 3. 나누기 연산
    var divisionValue = intValue / doubleValue
    // 4. 제곱 연산
    // python var expValue = intValue ** doubleValue
    var expValue = Math.pow(intValue, doubleValue)
    // 5. 나머지 연산
    var modValue = intValue%doubleValue

    // 문자열 연산하기
    var year = "2018"
    var week = "01W"
    var yearweek = year+week
    yearweek = "2018"+"01W"
    // 원하는 문자열 가져오기
    year = yearweek.substring(0,4)
    week = yearweek.substring(4,6)

    // 특정 구분자(delimeter) 활용하여 필요한 내용 추출하기
    var businessGroup = "IT;SERVICE;FINANCE"
    var businessGroupSet = businessGroup.split(";")
    // 문자열 길이 구하기
    businessGroup.length()
    // 특정 문자열 제외하기
    businessGroup= businessGroup.replace(";","")

    // 리스트 (가격정보 집합)
    var empty = List.empty
    var priceList = List(8000,9000,11000,10000,8500,1200,8000,9000,9500,11000)
    var complexList = List("SEC",(8000,9000))

    // 리스트 크기
    var listSize = priceList.size
    // 리스트 연산
    var minPrice = priceList.min
    var maxPrice = priceList.max
    // 리스트 추가
    priceList ++= List(9000)
    // 리스트 삭제
    priceList = priceList.filter(x=>{x!=minPrice})
    // 리스트 정렬
    priceList = priceList.sortBy(x=>{x})
    priceList = priceList.sortBy(x=>{-x})
    priceList = priceList.sortWith(_<_)
    priceList = priceList.sortWith(_>_)

    // 배열 (가격정보 집합)
    var emptyArray = Array.empty
    var priceArray = Array(8000,9000,11000,10000,8500,1200,8000,9000,9500,11000)

    // 배열 크기
    var ArraySize = priceArray.size
    // 배열 연산
    var minPriceArray = priceArray.min
    var maxPriceArray = priceArray.max
    // 배열 추가
    priceArray ++= Array(9000)
    // 배열 삭제
    priceArray = priceArray.filter(x=>{x!=minPriceArray})
    // 배열 업데이트
    priceArray.update(0,100000)

    // 배열 정렬
    priceArray = priceArray.sortBy(x=>{x})
    priceArray = priceArray.sortBy(x=>{-x})
    priceArray = priceArray.sortWith(_<_)
    priceArray = priceArray.sortWith(_>_)

  }
}