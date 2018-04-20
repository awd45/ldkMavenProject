package com.kopo

object Example_DataSaveWrite {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    var dataPath = "./data/"
    var selloutFile = "kopo_channel_seasonality.csv"

    // relative path
    //var selloutData1 = spark.read.format("csv").option("header", "true").load(dataPath + selloutFile)

    // absolute path
    var selloutData2 = spark.read.format("csv").option("header", "true").load("c:/spark/bin/data/" + selloutFile)

    selloutData2.createOrReplaceTempView("selloutTable")
    println(selloutData2.show)

    ///////////////////////////     데이터 파일 로딩 ////////////////////////////////////
    // 파일설정
    dataPath = "./data/"
    var paramFile = "KOPO_BATCH_SEASON_MPARA.txt"

    // 절대경로 입력
    var paramData= spark.read.format("csv").option("header","true").option("Delimiter",";").load("c:/spark/bin/data/"+paramFile)

    // 메모리 테이블 생성
    paramData.createOrReplaceTempView("paramTable")
    println(paramData.show)

    // Mysql Connection
    var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
    var staticUser = "root"
    var staticPw = "P@ssw0rd"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity)
    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")
    println(selloutDataFromMysql.show)
    println("mysql success")

    ///postgresql
    staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    staticUser = "kopo"
    staticPw = "kopo"
    selloutDb = "kopo_channel_seasonality"

    // jdbc (java database connectivity) 연결
    val selloutDataFromPg= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromPg.createOrReplaceTempView("selloutTable")
    println(selloutDataFromPg.show())
    println("postgres ok")

    // oracle connection
    staticUrl = "jdbc:oracle:thin:@192.168.0.10:1521/XE"

    staticUser = "kopo"
    staticPw = "kopo"
    selloutDb = "kopo_channel_seasonality"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("selloutTable")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    //mssql connection
    //    staticUrl = "jdbc:sqlserver://192.168.0.10;databaseName=kopo"
    //    staticUser = "haiteam"
    //    staticPw = "haiteam"
    //    selloutDb = "kopo_product_volume"
    //
    //    val selloutDataFromMssql = spark.read.format("jdbc").
    //      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
    //
    //    selloutDataFromMssql.createOrReplaceTempView("selloutTable")
    //
    //    println(selloutDataFromMssql.show())
    //    println("mssql ok")

    // data save
    // 데이터베이스 주소 및 접속정보 설정
    var myUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"

    // 데이터 저장
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", "kopo")
    prop.setProperty("password", "kopo")
    val table = "test1"
    //append
    selloutDataFromOracle.write.mode("overwrite").jdbc(myUrl, table, prop)


    // 파일저장
    selloutDataFromOracle.
      //coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("d:/savedData2/selloutDataFromOracle.csv") // 저장파일명

  }
}
