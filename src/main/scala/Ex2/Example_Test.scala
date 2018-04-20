package com.kopo

object Example_Test {
  def main(args: Array[String]): Unit = {


    // 정의 ####;##를 입력받아 ;를 기준으로 분류 후 year, week를 분리한다.
    // 입력
    def quizDef(inputValue: String): (Int, Int) = {

      var inputValue = "2017;34"
      var target = inputValue.split(";")
      var yearValue = target(0)
      var weekValue = target(1)
      (yearValue.toInt, weekValue.toInt)
    }

    var test = "2017;34"

    var answer = quizDef(test)



  }
}