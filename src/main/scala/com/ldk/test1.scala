package com.ldk
import org.apache.spark.sql.SparkSession
object test1 {
  def main(args: Array[String]): Unit = {

          val spark = SparkSession.builder().appName("hkProject").
            config("spark.master", "local").
            getOrCreate()


           // 접속정보 설정
        var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
        var staticUser = "kopo"
           var staticPw = "kopo"
           var selloutDb = "kopo_channel_seasonality_ex"


           // jdbc (java database connectivity) 연결
           val selloutDataFromPg= spark.read.format("jdbc").
             options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load


           // 메모리 테이블 생성
           selloutDataFromPg.createOrReplaceTempView("selloutTable")
           selloutDataFromPg.show(1)


         }

}
