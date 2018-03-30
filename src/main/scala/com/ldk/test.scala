package com.ldk

import com.dg

object test {

  def main(args: Array[String]): Unit = {
    def dground(targetValue:Double, sequence:Int): Double = {
      var multiValue = Math.pow(10,sequence)
      var returnValue = Math.round(targetValue*multiValue)/multiValue
      returnValue
    }

    var testValue = 12.222222
    var answer = dground(testValue,3)
    println(answer)
  }

}
