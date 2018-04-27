package com.ldg

import org.apache.spark.sql.SparkSession

object Example_load {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var a = 10;
    println(10);

    var testArray = (22,33,44,55,60,70)

//    var answer = testArray.filter(x=>{x%10 == 0})
//
//    var answer = testArray.filter(x=>{
//      var data = x.toString
//      var dataSize = data.size
//
//      var lastChar = data.substring(dataSize - 1).toString
//
//      lastChar.equalsIgnoreCase("0")
//    })
//
//    var arraySize = answer.size
//    for(i<-0 until arraySize){
//      println(answer(i))
//    }
  }

}
