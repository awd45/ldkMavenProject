package com.kopo

import org.apache.spark.sql.SparkSession

object Example_Basic {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    //Load data
    var testData = spark.read.text("c:/spark/README.md")
    //Save data
    testData.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      save("test") // 저장파일명
    println("spark test completed")

  }

}