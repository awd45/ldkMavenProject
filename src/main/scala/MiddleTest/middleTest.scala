package MiddleTest

import org.apache.spark.sql.SparkSession

object middleTest {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("hkProject").

      config("spark.master", "local").

      getOrCreate()



    // 1번문제 로딩
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"

    var staticUser = "kopo"

    var staticPw = "kopo"

    var selloutDb = "kopo_channel_seasonality_new"

    // jdbc (java database connectivity) 연결

    val selloutDf = spark.read.format("jdbc").

      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성

    selloutDf.createOrReplaceTempView("selloutTable")

    //1번 답
    selloutDf.show(2)


    //2번 답 SQL분석
    var middleResult = spark.sql("select regionid, product, yearweek, cast(qty as double), cast(QTY * 1.2 as double)AS QTY_NEW FROM selloutTable")




    //4번답 정제
    var rawData = middleResult
    var rawDataColumns = rawData.columns.map(x=>{x.toLowerCase()})

    var accountidNo = rawDataColumns.indexOf("regionid")

    var productNo = rawDataColumns.indexOf("product")

    var yearweekNo = rawDataColumns.indexOf("yearweek")

    var qtyNo = rawDataColumns.indexOf("qty")

    var qtyNewNo = rawDataColumns.indexOf("qty_new")


    //Rdd로 변환
    var rawRdd = rawData.rdd

    var x = rawRdd.first

    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = false;
      var yearValue = x.getString(yearweekNo).substring(0,4).toInt

      if ( ((x.getString(productNo) == "PRODUCT1") ||
            (x.getString(productNo) == "PRODUCT2")) &&
            (yearValue >= 2016)) {
        checkValid = true;
      }
      checkValid
    })

    var filteredRdd2 = filteredRdd.filter(x=>{
      var checkValid = true;
      var weekValue = x.getString(yearweekNo).substring(4).toInt
      if (weekValue ==52){
        checkValid = false;

      }

      checkValid

    })



    //5번 저장
    import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

    val finalResultDf = spark.createDataFrame(filteredRdd2,

      StructType(

        Seq(
          StructField("regionid", StringType),

          StructField("product", StringType),

          StructField("yearweek", StringType),

          StructField("qty", DoubleType),

          StructField("qty_new", DoubleType))))

    finalResultDf.createOrReplaceTempView("finalResultDfTb")
    finalResultDf.show(2)


    var myUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var outputUser = "kopo"
    var outputPw = "kopo"

    val prop = new java.util.Properties

    prop.setProperty("driver", "org.postgresql.Driver")

    prop.setProperty("user", "kopo")

    prop.setProperty("password", "kopo")

    val table = "kopo_st_result_ldg"

    finalResultDf.write.mode("overwrite").jdbc(myUrl, table, prop)

  }
}
