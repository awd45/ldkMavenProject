package com.kopo

import org.apache.spark.sql.SparkSession

object Example_Function {

  def main(args: Array[String]): Unit = {

    //    val spark = SparkSession.builder().appName("hkProject").
    //      config("spark.master", "local").
    //      getOrCreate()

    println("================== Basic Logic =======================")
    // first logic
    var a = 15.125222
    var b = 15.147218
    var c = 69.72756
    var limitedValue1 = 100.0
    var ra1 = Math.round(a*100)/100.0
    var rb1 = Math.round(b*100)/100.0
    var rc1 = Math.round(c*100)/100.0
    var sum1Before = ra1+rb1+rc1

    var error1 = Math.round((limitedValue1-(ra1+rb1+rc1))*100d)/100d

    var ra_new1 = ra1+(error1)

    var sum1 = ra_new1+rb1+rc1
    println("ra1+ra2+rc1= "+sum1Before)
    println("sum1 is "+sum1)

    // second logic
    println("================== Function Logic =======================")

    def roundDef(inputValue: Double, sequence: Int): Double = {

      var multiValue = Math.pow(10,sequence)
      var result = Math.round(inputValue*multiValue)/multiValue
      result
    }
    var limitedValue2 = 100.0
    var ra2 = roundDef(a,2)
    var rb2 = roundDef(b,2)
    var rc2 = roundDef(c,2)

    var error2 = roundDef(limitedValue2-(ra2+rb2+rc2),2)

    var ra_new2 = ra2+(error2)

    var sum2 = ra_new2+rb2+rc2
    var sum2Before = ra2+rb2+rc2
    println("ra2+rb2+rc2= "+sum2Before)
    println("sum2 is "+sum2)


  }

}